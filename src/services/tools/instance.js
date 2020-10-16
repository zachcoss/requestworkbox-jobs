const
    _ = require('lodash'),
    moment = require('moment'),
    Axios = require('axios'),
    indexSchema = require('../schema/indexSchema'),
    async = require('async'),
    asyncEachOf = async.eachSeries,
    Agent = require('agentkeepalive'),
    keepAliveAgent = new Agent({
        maxSockets: 100,
        maxFreeSockets: 10,
        timeout: 60000, // active socket keepalive for 60 seconds
        freeSocketTimeout: 30000, // free socket keepalive for 30 seconds
    }),
    axios = Axios.create({httpAgent: keepAliveAgent}),
    socketService = require('./socket'),
    S3 = require('../tools/s3').S3;

module.exports = {
    // incoming fields is the request payload body
    // sent through start-workflow and return-workflow
    start: async (instanceId, incomingFields) => {

        const snapshot = {}
        const state = {
            instance: {},
            workflow: {},
            requests: {},
            storages: {},
        }

        const getFunctions = {
            getInstance: async function() {
                const instance = await indexSchema.Instance.findById(instanceId)
                state.instance = instance
            },
            getWorkflow: async function() {
                const workflow = await indexSchema.Workflow.findById(state.instance.workflow, '', {lean: true})
                state.workflow = workflow
            },
            getRequests: async function() {
                await asyncEachOf(state.workflow.tasks, async function (task, index) {
                    if (!task.requestId || task.requestId === '') return;
                    if (state.requests[task.requestId]) return;
                    const request = await indexSchema.Request.findById(task.requestId, '', {lean: true})
                    state.requests[task.requestId] = request
                });
            },
            getStorages: async function() {
                await asyncEachOf(state.requests, async function(request) {
                    await asyncEachOf(request.query, async function (obj) {
                        if (obj.valueType !== 'storage') return;
                        if (state.storages[obj.value]) return;

                        const storage = await indexSchema.Storage.findById(obj.value, 'storageType mimetype size', {lean: true})
                        state.storages[obj.value] = storage
                    })
                    await asyncEachOf(request.headers, async function (obj) {
                        if (obj.valueType !== 'storage') return;
                        if (state.storages[obj.value]) return;

                        const storage = await indexSchema.Storage.findById(obj.value, 'storageType mimetype size', {lean: true})
                        state.storages[obj.value] = storage
                    })
                    await asyncEachOf(request.body, async function (obj) {
                        if (obj.valueType !== 'storage') return;
                        if (state.storages[obj.value]) return;
                        
                        const storage = await indexSchema.Storage.findById(obj.value, 'storageType mimetype size', {lean: true})
                        state.storages[obj.value] = storage
                    })
                })
            },
            getStorageDetails: async function() {
                await asyncEachOf(state.storages, async function(storage) {
                    const storageValue = await S3.getObject({
                        Bucket: "connector-storage",
                        Key: `${state.instance.sub}/storage/${storage._id}`,
                    }).promise()

                    if (storage.storageType === 'text') {
                        const fullStorageValue = String(storageValue.Body)
                        storage.storageValue = fullStorageValue
                    } else if (storage.storageType === 'file') {
                        if (storage.mimetype === 'text/plain') {
                            storage.storageValue = String(storageValue.Body)
                        } else if (storage.mimetype === 'application/json') {
                            storage.storageValue = JSON.parse(storageValue.Body)
                        }
                    }

                    const usage = new indexSchema.Usage({
                        sub: state.instance.sub,
                        usageType: 'storage',
                        usageDirection: 'down',
                        usageAmount: Number(storage.size),
                        usageLocation: 'instance'
                    })
                    await usage.save()
                })
            }
        }

        const templateFunctions = {
            templateInputs: function(requestId, inputs = {}) {
                const requestTemplate = {
                    requestId: requestId,
                    url: {
                        method: '',
                        url: '',
                        name: ''
                    },
                    query: {},
                    headers: {},
                    body: {}
                }

                const request = state.requests[requestId]
                const requestDetails = _.pick(request, ['query','headers','body'])

                

                // Apply url
                _.each(request.url, (value, key) => {
                    requestTemplate.url[key] = value
                })

                // Apply inputs
                _.each(requestDetails, (requestDetailArray, requestDetailKey) => {
                    _.each(requestDetailArray, (requestDetailObj) => {
                        if (requestDetailObj.key === '') return;
                        
                        if (requestDetailObj.valueType === 'textInput') {
                            requestTemplate[requestDetailKey][requestDetailObj.key] = requestDetailObj.value
                        } else if (requestDetailObj.valueType === 'storage') {
                            const storageId = requestDetailObj.value
                            const storageValue = state.storages[storageId].storageValue
                            requestTemplate[requestDetailKey][requestDetailObj.key] = storageValue
                        } else if (requestDetailObj.valueType === 'runtimeResult') {
                            const runtimeResultName = requestDetailObj.value
                            _.each(snapshot, (task) => {
                                if (task.request.url.name === runtimeResultName) {
                                    requestTemplate[requestDetailKey][requestDetailObj.key] = task.response
                                }
                            })
                        } else if (requestDetailObj.valueType === 'incomingField') {
                            const incomingFieldName = requestDetailObj.value
                            if (incomingFields[incomingFieldName]) {
                                requestTemplate[requestDetailKey][requestDetailObj.key] = incomingFields[incomingFieldName]
                            }  
                        }
                    })
                })

                return requestTemplate
            },
        }

        const statFunctions = {
            createStat: async function(statConfig) {
                try {
                    console.log('db stat start')
                    const dbStatStart = new Date()
                    // Stat for Db
                    const safeStat = _.omit(statConfig, ['requestPayload','responsePayload', 'headers'])
                    const dbStat = indexSchema.Stat(safeStat)
                    await dbStat.save()
                    const dbStatEnd = new Date()
                    console.log('db stat end', dbStatEnd - dbStatStart)

                    console.log('instance stat start')
                    const instanceStatStart = new Date()
                    state.instance.stats.push(dbStat._id)
                    await state.instance.save()
                    const instanceStatEnd = new Date()
                    console.log('instance stat end', instanceStatEnd - instanceStatStart)

                    console.log('s3 stat start')
                    const s3StatStart = new Date()
                    // Stat for S3
                    const completeStat = _.assign(statConfig, {_id: dbStat._id})
                    await S3.upload({
                        Bucket: "connector-storage",
                        Key: `${state.instance.sub}/instance-statistics/${completeStat.instance}/${completeStat._id}`,
                        Body: JSON.stringify(completeStat)
                    }).promise()
                    const s3StatEnd = new Date()
                    console.log('s3 stat end', s3StatEnd - s3StatStart)

                    console.log('socket start')
                    const socketStart = new Date()
                    // Emit
                    socketService.io.emit(state.instance.sub, {
                        eventDetail: 'Running...',
                        instanceId: state.instance._id,
                        workflowName: state.workflow.name,
                        requestName: safeStat.requestName,
                        statusCode: safeStat.status,
                        duration: statConfig.duration,
                        responseSize: statConfig.responseSize,
                        message: 'Request Complete',
                    });

                    const socketEnd = new Date()
                    console.log('socket end', socketEnd - socketStart)

                } catch(err) {
                    console.log('create stat error', err)
                    throw new Error('Error creating stat')
                }
            }
        }

        const runFunctions = {
            runRequest: async function(requestTemplate, requestType) {
                const requestConfig = {
                    url: requestTemplate.url.url,
                    method: requestTemplate.url.method,
                    headers: requestTemplate.headers,
                    // axios requires params field rather than query
                    params: requestTemplate.query,
                    // axios requires data field rather than body
                    data: requestTemplate.body,
                }
                const statConfig = {
                    instance: instanceId,
                    requestName: requestTemplate.url.name,
                    requestType: requestType,
                    requestId: requestTemplate.requestId,
                    requestPayload: requestConfig,
                    responsePayload: {},
                    status: 0,
                    statusText: '',
                    startTime: new Date(),
                    endTime: new Date(),
                }
                try {
                    console.log('request start')
                    const requestStart = new Date()
                    const request = await axios(requestConfig)
                    const requestEnd = new Date()
                    const requestTime = requestEnd - requestStart
                    console.log('request end', requestTime)

                    const requestLength = request.config.headers['Content-Length']
                    const responseLength = request.headers['content-length']

                    const requestUsageUp = new indexSchema.Usage({
                        sub: state.instance.sub,
                        usageType: 'request',
                        usageDirection: 'up',
                        usageAmount: Number(requestLength),
                        usageLocation: 'instance'
                    })
                    await requestUsageUp.save()

                    const requestUsageDown = new indexSchema.Usage({
                        sub: state.instance.sub,
                        usageType: 'request',
                        usageDirection: 'down',
                        usageAmount: Number(responseLength),
                        usageLocation: 'instance'
                    })
                    await requestUsageDown.save()

                    const requestUsageTime = new indexSchema.Usage({
                        sub: state.instance.sub,
                        usageType: 'request',
                        usageDirection: 'time',
                        usageAmount: Number(requestTime),
                        usageLocation: 'instance'
                    })
                    await requestUsageTime.save()

                    const requestResults = _.pick(request, ['data', 'status', 'statusText','headers'])
                    
                    statConfig.responsePayload = requestResults.data
                    statConfig.status = requestResults.status
                    statConfig.statusText = requestResults.statusText
                    statConfig.endTime = new Date()
                    statConfig.duration = requestEnd - requestStart
                    statConfig.responseSize = requestResults.headers['content-length']
                    statConfig.responseType = requestResults.headers['content-type']

                    await statFunctions.createStat(statConfig)

                    return requestResults
                } catch(err) {
                    console.log('request error', err)
                    throw new Error(err)
                }
            },
        }

        const processFunctions = {
            processRequestResponse: async function(requestResponse, taskId) {
                // console.log('request response', requestResponse)
                snapshot[taskId].response = requestResponse.data
            },
        }

        const initFunctions = {
            initializeRequest: async function(taskId, requestId, inputs) {
                // apply inputs
                const requestTemplate = await templateFunctions.templateInputs(requestId, inputs)
                // initialize snapshot
                snapshot[taskId] = {
                    request: requestTemplate,
                    response: {},
                }
            },
        }

        const startFunctions = {
            startRequest: async function(taskId) {
                const requestTemplate = snapshot[taskId].request
                // perform request
                const requestResponse = await runFunctions.runRequest(requestTemplate, 'request')
                // perform updates
                processFunctions.processRequestResponse(requestResponse, taskId)
            },

            startWorkflow: async function() {
                for (const task of state.workflow.tasks) {
                    const request = state.requests[task.requestId]
                    await initFunctions.initializeRequest(task._id, task.requestId, task.inputs)
                    await startFunctions.startRequest(task._id)
                }
            },
        }

        const init = async () => {

            // initialize state
            await getFunctions.getInstance()
            await getFunctions.getWorkflow()

            socketService.io.emit(state.instance.sub, {
                eventDetail: 'Loading...',
                instanceId: state.instance._id,
                workflowName: state.workflow.name,
                requestName: '',
                statusCode: '',
                duration: '',
                responseSize: '',
                message: '',
            });

            await getFunctions.getRequests()
            await getFunctions.getStorages()
            await getFunctions.getStorageDetails()

            socketService.io.emit(state.instance.sub, {
                eventDetail: 'Initializing...',
                instanceId: state.instance._id,
                workflowName: state.workflow.name,
                requestName: '',
                statusCode: '',
                duration: '',
                responseSize: '',
                message: '',
            });

            // start workflow
            await startFunctions.startWorkflow()

            socketService.io.emit(state.instance.sub, {
                eventDetail: 'Complete',
                instanceId: state.instance._id,
                workflowName: state.workflow.name,
                requestName: '',
                statusCode: '',
                duration: '',
                responseSize: '',
                message: '',
            });

            return snapshot
        }

        try {
            console.log('instance start')
            const finalSnapshot = await init()
            console.log('instance complete')
            // console.log(finalSnapshot)
            return finalSnapshot
        } catch(err) {
            console.log('err', err)
            return err
        }

    },
}