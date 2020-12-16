const
    _ = require('lodash'),
    moment = require('moment'),
    Axios = require('axios'),
    IndexSchema = require('../tools/schema').schema,
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
    socketService = require('../tools/socket'),
    Stats = require('../tools/stats').stats,
    S3 = require('./s3').S3,
    SQS = require('./sqs').SQS;

module.exports = {
    start: async (queuePayload, queueDoc, messageAction) => {

        if (!queuePayload && !queueDoc) {
            console.log('Missing queue information')
            throw new Error('Missing queue information')
        }

        const incoming = {
            queueId: false,
            ReceiptHandle: false,
        }
        
        if (!queuePayload && queueDoc._id) incoming.queueId = queueDoc._id
        if (queuePayload && queuePayload.Body) incoming.queueId = queuePayload.Body.split(' ')[0]
        if (queuePayload && queuePayload.ReceiptHandle) incoming.ReceiptHandle = queuePayload.ReceiptHandle

        if (messageAction === 'extend' && incoming.ReceiptHandle) {
            // update visibility timeout
            await SQS.changeMessageVisibility({
                QueueUrl: process.env.AWS_QUEUE_STANDARD_URL,
                ReceiptHandle: incoming.ReceiptHandle,
                VisibilityTimeout: 30,
            }).promise()

            throw new Error('Request is running')
        }

        const snapshot = {
            payloads: [],
            tasks: [],
            webhooks: [],
        }
        const state = {
            queue: {},
            instance: {},
            workflow: {},
            requests: {},
            storages: {},
            statuscheck: {},
            runtimeResultNames: {},
            webhookRequestId: null,
            webhookTaskId: null,
        }

        const queueFunctions = {
            getQueue: async function() {
                const queue = await IndexSchema.Queue.findById(incoming.queueId)
                state.queue = queue
            },
            processQueue: async function() {
                const queueStatus = state.queue.status
                if (queueStatus === 'queued') {
                    // update status
                    await Stats.updateQueueStats({ queue: state.queue, status: 'starting', }, IndexSchema, socketService)
                } else if (queueStatus === 'complete' || queueStatus === 'error' || queueStatus === 'archived') {
                    // delete message
                    await SQS.deleteMessage({
                        QueueUrl: process.env.AWS_QUEUE_STANDARD_URL,
                        ReceiptHandle: incoming.ReceiptHandle,
                    }).promise()

                    throw new Error('Request completed')
                } else {
                    // update visibility timeout
                    await SQS.changeMessageVisibility({
                        QueueUrl: process.env.AWS_QUEUE_STANDARD_URL,
                        ReceiptHandle: incoming.ReceiptHandle,
                        VisibilityTimeout: 30,
                    }).promise()

                    throw new Error('Request is running')
                }
            },
            getStatuscheck: async function() {
                if (state.queue.queueType === 'statuscheck') {
                    const statuscheck = await IndexSchema.Statuscheck.findOne(state.queue.statuscheckId)
                    state.statuscheck = statuscheck
                }
            },
            getBodyPayload: async function() {
                if (state.queue.storageInstanceId && state.queue.storageInstanceId !== '') {
                    const payloadTaskId = state.workflow.payloads[0]._id

                    // pull body payload
                    const bodyPayloadStart = new Date()
                    const storageValue = await S3.getObject({
                        Bucket: process.env.STORAGE_BUCKET,
                        Key: `${state.queue.sub}/request-payloads/${state.queue.storageInstanceId}`,
                    }).promise()

                    // add payload to snapshot
                    processFunctions.processRequestResponse(JSON.parse(storageValue.Body), payloadTaskId, 'payloads')

                    const bodyPayloadSize = Buffer.byteLength(JSON.stringify(storageValue.Body), 'utf8')

                    const requestResultTime = new Date() - bodyPayloadStart

                    const usages = [{
                        sub: state.instance.sub,
                        usageType: 'storage',
                        usageDirection: 'down',
                        usageAmount: bodyPayloadSize,
                        usageMeasurement: 'byte',
                        usageLocation: 'instance',
                        usageId: state.instance._id,
                        usageDetail: `Body payload download`,
                    }, {
                        sub: state.instance.sub,
                        usageType: 'storage',
                        usageDirection: 'time',
                        usageAmount: Number(requestResultTime),
                        usageMeasurement: 'ms',
                        usageLocation: 'instance',
                        usageId: state.instance._id,
                        usageDetail: `Body payload download`,
                    }]
        
                    await Stats.updateInstanceUsage({ instance: state.instance, usages, }, IndexSchema)

                    // Create stat
                    const statConfig = {
                        instanceId: state.instance._id,
                        requestName: `Body Payload`,
                        requestType: 'bodyPayload',
                        requestId: state.queue.storageInstanceId,
                        requestPayload: {},
                        responsePayload: JSON.parse(storageValue.Body),
                        taskId: payloadTaskId,
                        taskField: 'payloads',
                        status: 200,
                        statusText: 'OK',
                        startTime: bodyPayloadStart,
                        endTime: new Date(),
                        duration: Number(requestResultTime),
                        responseSize: bodyPayloadSize,
                    }

                    await statFunctions.createStat(statConfig)

                }
            },
        }

        const getFunctions = {
            getInstance: async function() {
                // Add sub
                const instance = await IndexSchema.Instance.findOne({ _id: state.queue.instanceId, sub: state.queue.sub })
                state.instance = instance
            },
            getWorkflow: async function() {
                // Add sub
                const workflow = await IndexSchema.Workflow.findOne({ _id: state.instance.workflowId, sub: state.queue.sub }, '', {lean: true})
                state.workflow = workflow

                _.each(state.workflow.tasks, (task) => {
                    if (task.runtimeResultName && task.runtimeResultName !== '') {
                        state.runtimeResultNames[task.runtimeResultName] = task._id
                    }
                })
            },
            getRequests: async function() {
                // Requests
                await asyncEachOf(state.workflow.tasks, async function (task, index) {
                    if (!task.requestId || task.requestId === '') return;
                    if (state.requests[task.requestId]) return;
                    // Add sub
                    const request = await IndexSchema.Request.findOne({ _id: task.requestId, sub: state.queue.sub }, '', {lean: true})
                    state.requests[task.requestId] = request
                })
                // Webhooks
                await asyncEachOf(state.workflow.webhooks, async function (webhook, index) {
                    if (!webhook.requestId || webhook.requestId === '') return;
                    if (state.requests[webhook.requestId]) return;
                    // Add sub
                    const request = await IndexSchema.Request.findOne({ _id: webhook.requestId, sub: state.queue.sub }, '', {lean: true})
                    state.requests[webhook.requestId] = request
                    
                    state.webhookRequestId = webhook.requestId
                    state.webhookTaskId = webhook._id
                })
            },
            getStorages: async function() {
                await asyncEachOf(state.requests, async function(request) {
                    await asyncEachOf(request.query, async function (obj) {
                        if (obj.valueType !== 'storage') return;
                        if (state.storages[obj.value]) return;

                        // Add sub
                        const storage = await IndexSchema.Storage.findOne({ _id: obj.value, sub: state.queue.sub }, 'storageType mimetype size name', {lean: true})
                        state.storages[obj.value] = storage
                    })
                    await asyncEachOf(request.headers, async function (obj) {
                        if (obj.valueType !== 'storage') return;
                        if (state.storages[obj.value]) return;

                        // Add sub
                        const storage = await IndexSchema.Storage.findOne({ _id: obj.value, sub: state.queue.sub }, 'storageType mimetype size name', {lean: true})
                        state.storages[obj.value] = storage
                    })
                    await asyncEachOf(request.body, async function (obj) {
                        if (obj.valueType !== 'storage') return;
                        if (state.storages[obj.value]) return;
                        
                        // Add sub
                        const storage = await IndexSchema.Storage.findOne({ _id: obj.value, sub: state.queue.sub }, 'storageType mimetype size name', {lean: true})
                        state.storages[obj.value] = storage
                    })
                })
            },
            getStorageDetails: async function() {
                await asyncEachOf(state.storages, async function(storage) {
                    const storageValueStart = new Date()
                    const storageValue = await S3.getObject({
                        Bucket: process.env.STORAGE_BUCKET,
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

                    const storageValueSize = Buffer.byteLength(JSON.stringify(storage.storageValue), 'utf8')

                    const usages = [{
                        sub: state.instance.sub,
                        usageType: 'storage',
                        usageDirection: 'down',
                        usageAmount: storageValueSize,
                        usageMeasurement: 'byte',
                        usageLocation: 'instance',
                        usageId: state.instance._id,
                        usageDetail: `Storage download: ${storage.name}`,
                    }, {
                        sub: state.instance.sub,
                        usageType: 'storage',
                        usageDirection: 'time',
                        usageAmount: Number(new Date() - storageValueStart),
                        usageMeasurement: 'ms',
                        usageLocation: 'instance',
                        usageId: state.instance._id,
                        usageDetail: `Storage download: ${storage.name}`,
                    }]
        
                    await Stats.updateInstanceUsage({ instance: state.instance, usages, }, IndexSchema)
                })
            }
        }

        const templateFunctions = {
            templateInputs: function(requestId, taskId, taskField) {
                const requestTemplate = {
                    requestId: requestId,
                    taskId: taskId,
                    taskField: taskField,
                    url: '',
                    method: '',
                    name: '',
                    query: {},
                    headers: {},
                    body: {}
                }

                const request = state.requests[requestId]
                const requestDetails = _.pick(request, ['query','headers','body'])

                requestTemplate.url = request.url
                requestTemplate.name = request.name
                requestTemplate.method = request.method

                // Apply inputs
                _.each(requestDetails, (requestDetailArray, requestDetailKey) => {
                    _.each(requestDetailArray, (requestDetailObj) => {
                        if (requestDetailObj.key === '') return
                        
                        if (requestDetailObj.valueType === 'textInput') {
                            requestTemplate[requestDetailKey][requestDetailObj.key] = requestDetailObj.value
                        } else if (requestDetailObj.valueType === 'storage') {
                            const storageId = requestDetailObj.value
                            const storageValue = state.storages[storageId].storageValue
                            requestTemplate[requestDetailKey][requestDetailObj.key] = storageValue
                        } else if (requestDetailObj.valueType === 'runtimeResult') {
                            if (requestDetailObj.value === '') return
                            if (!state.runtimeResultNames[requestDetailObj.value]) return

                            let taskId = state.runtimeResultNames[requestDetailObj.value]

                            _.each(snapshot.tasks, (taskObj) => {
                                if (taskObj._id === taskId && taskObj.responsePayload) {
                                    requestTemplate[requestDetailKey][requestDetailObj.key] = taskObj.responsePayload
                                }
                            })
                        } else if (requestDetailObj.valueType === 'incomingField') {
                            const incomingFieldName = requestDetailObj.value
                            _.each(snapshot.payloads, (payloadObj) => {
                                if (payloadObj.responsePayload && payloadObj.responsePayload[incomingFieldName]) {
                                    requestTemplate[requestDetailKey][requestDetailObj.key] = payloadObj.responsePayload[incomingFieldName]
                                }
                            })
                        }
                    })
                })

                // Apply webhook
                if (taskField === 'webhooks') {
                    if (!_.size(requestTemplate.body)) {
                        requestTemplate.body = snapshot
                    } else {
                        requestTemplate.body['snapshot'] = snapshot
                    }
                }

                return requestTemplate
            },
        }

        const runFunctions = {
            runRequest: async function(requestTemplate, requestType) {
                let requestConfig = {
                    url: requestTemplate.url,
                    method: requestTemplate.method,
                    headers: requestTemplate.headers,
                    // axios requires params field rather than query
                    params: requestTemplate.query,
                    // axios requires data field rather than body
                    data: requestTemplate.body,
                }
                
                const statConfig = {
                    instanceId: state.instance._id,
                    requestName: requestTemplate.name,
                    requestType: requestType,
                    requestId: requestTemplate.requestId,
                    taskId: requestTemplate.taskId,
                    taskField: requestTemplate.taskField,
                    requestPayload: requestConfig,
                    responsePayload: {},
                    status: 0,
                    statusText: '',
                    startTime: new Date(),
                    endTime: new Date(),
                }
                
                try {

                    const requestLengthSize = Buffer.byteLength(JSON.stringify(requestConfig), 'utf8')

                    const requestUsages = [{
                        sub: state.instance.sub,
                        usageType: 'request',
                        usageDirection: 'up',
                        usageAmount: requestLengthSize,
                        usageMeasurement: 'byte',
                        usageLocation: 'instance',
                        usageId: state.instance._id,
                        usageDetail: `Request: ${statConfig.requestName}`,
                    }]
        
                    await Stats.updateInstanceUsage({ instance: state.instance, usages: requestUsages, }, IndexSchema)

                    const requestStart = new Date()
                    const request = await axios(requestConfig)
                    const requestEnd = new Date()
                    const requestResultTime = requestEnd - requestStart

                    const requestResults = _.pick(request, ['data', 'status', 'statusText','headers'])
                    const resultLengthSize = Buffer.byteLength(JSON.stringify(requestResults), 'utf8')

                    const responseUsages = [{
                        sub: state.instance.sub,
                        usageType: 'request',
                        usageDirection: 'down',
                        usageAmount: resultLengthSize,
                        usageMeasurement: 'byte',
                        usageLocation: 'instance',
                        usageId: state.instance._id,
                        usageDetail: `Response: ${statConfig.requestName}`,
                    }, {
                        sub: state.instance.sub,
                        usageType: 'request',
                        usageDirection: 'time',
                        usageAmount: requestResultTime,
                        usageMeasurement: 'ms',
                        usageLocation: 'instance',
                        usageId: state.instance._id,
                        usageDetail: `Response: ${statConfig.requestName}`,
                    }]
        
                    await Stats.updateInstanceUsage({ instance: state.instance, usages: responseUsages, }, IndexSchema)

                    statConfig.requestSize = requestLengthSize
                    statConfig.responsePayload = requestResults.data
                    statConfig.status = requestResults.status
                    statConfig.statusText = requestResults.statusText
                    statConfig.endTime = new Date()
                    statConfig.duration = requestResultTime
                    statConfig.responseSize = resultLengthSize
                    statConfig.responseType = requestResults.headers['content-type']

                    await statFunctions.createStat(statConfig)

                    return requestResults
                } catch(err) {
                    console.log('request error', err.message || err.response || err)

                    statConfig.status = (err.response && err.response.status) ? err.response.status : 500
                    statConfig.statusText = (err.response && err.response.statusText) ? err.response.statusText : err.message
                    statConfig.error = true

                    await statFunctions.createStat(statConfig)

                    if (state.statuscheck) {
                        if (state.statuscheck && state.statuscheck.onWorkflowTaskError && state.statuscheck.onWorkflowTaskError === 'continue') {
                            console.log('continue after error')
                        } else {
                            throw new Error(err)
                        }
                    } else {
                        throw new Error(err)
                    }
                }
            },
        }

        const statFunctions = {
            createStat: async function(statConfig) {
                try {
                    if (state.statuscheck && state.statuscheck._id) {
                        await Stats.updateInstanceStats({ instance: state.instance, statConfig, }, IndexSchema, S3, process.env.STORAGE_BUCKET, socketService, state.statuscheck)
                    } else {
                        await Stats.updateInstanceStats({ instance: state.instance, statConfig, }, IndexSchema, S3, process.env.STORAGE_BUCKET)
                    }
                } catch(err) {
                    console.log('create stat error', err)
                    throw new Error('Error creating stat')
                }
            }
        }

        const processFunctions = {
            processRequestResponse: async function(requestResponse, taskId, taskField) {
                if (taskField === 'payloads') {
                    snapshot[taskField].push({
                        _id: taskId,
                        responsePayload: requestResponse
                    })
                } else {
                    if (requestResponse && requestResponse.data) {
                        snapshot[taskField].push({
                            _id: taskId,
                            responsePayload: requestResponse.data
                        })
                    } else {
                        snapshot[taskField].push({
                            _id: taskId,
                            responsePayload: 'Error'
                        })
                    }
                }
            },
        }

        const startFunctions = {
            startWorkflow: async function() {
                for (const task of state.workflow.tasks) {
                    const requestTemplate = await templateFunctions.templateInputs(task.requestId, task._id, 'tasks')
                    const requestResponse = await runFunctions.runRequest(requestTemplate, 'request')
                    processFunctions.processRequestResponse(requestResponse, task._id, 'tasks')
                }
            },
            sendWebhook: async function() {
                if (!state.webhookRequestId || state.webhookRequestId === '') return;
                
                const startWebhook = new Date()
    
                const requestTemplate = await templateFunctions.templateInputs(state.webhookRequestId, state.webhookTaskId, 'webhooks')
                const requestResponse = await runFunctions.runRequest(requestTemplate, 'webhook')
                processFunctions.processRequestResponse(requestResponse, state.webhookTaskId, 'webhooks')
    
                const endWebhook = new Date()
            },
        }

        const init = async () => {

            // initialize queue state
            await queueFunctions.getQueue()
            await queueFunctions.processQueue()

            await Stats.updateQueueStats({ queue: state.queue, status: 'initializing', }, IndexSchema, socketService)

            // get status check
            await queueFunctions.getStatuscheck()
            
            // initialize instance state
            await getFunctions.getInstance()
            await getFunctions.getWorkflow()

            await Stats.updateQueueStats({ queue: state.queue, status: 'loading', }, IndexSchema, socketService)
            
            await getFunctions.getRequests()
            await getFunctions.getStorages()
            await getFunctions.getStorageDetails()
            await queueFunctions.getBodyPayload()

            await Stats.updateQueueStats({ queue: state.queue, status: 'running', }, IndexSchema, socketService)

            // start workflow
            await startFunctions.startWorkflow()

            if (incoming.ReceiptHandle) {
                // Delete message
                await SQS.deleteMessage({
                    QueueUrl: process.env.AWS_QUEUE_STANDARD_URL,
                    ReceiptHandle: incoming.ReceiptHandle,
                }).promise()

                // Send webhook
                if (state.webhookRequestId) {
                    if (state.statuscheck && state.statuscheck.sendWorkflowWebhook) {
                        if (state.statuscheck.sendWorkflowWebhook === 'always') {
                            await Stats.updateQueueStats({ queue: state.queue, status: 'webhook', }, IndexSchema, socketService)
                            await startFunctions.sendWebhook()
                        } else if  (state.statuscheck.sendWorkflowWebhook === 'onSuccess') {
                            await Stats.updateQueueStats({ queue: state.queue, status: 'webhook', }, IndexSchema, socketService)
                            await startFunctions.sendWebhook()
                        }
                    } else {
                        await Stats.updateQueueStats({ queue: state.queue, status: 'webhook', }, IndexSchema, socketService)
                        await startFunctions.sendWebhook()
                    }
                }

                await Stats.updateQueueStats({ queue: state.queue, status: 'complete', }, IndexSchema, socketService)
            } else {
                await Stats.updateQueueStats({ queue: state.queue, status: 'complete', }, IndexSchema, socketService)
            }

            return snapshot
        }

        try {
            const finalSnapshot = await init()

            return finalSnapshot
            
        } catch(err) {
            console.log('err', err.message || err.response || err)

            if (err.message === 'Request completed' || err.message === 'Request is running') {
                return
            } else {
                if (incoming.ReceiptHandle) {
                    // Delete message
                    await SQS.deleteMessage({
                        QueueUrl: process.env.AWS_QUEUE_STANDARD_URL,
                        ReceiptHandle: incoming.ReceiptHandle,
                    }).promise()

                    if (state.webhookRequestId && state.queue.status === 'webhook') {
                        // Update stat
                        await Stats.updateQueueStats({ queue: state.queue, status: 'error', statusText: err.message }, IndexSchema, socketService)
                    } else {
                        await Stats.updateQueueStats({ queue: state.queue, status: 'error', statusText: err.message }, IndexSchema, socketService)
                        
                        // Send webhook
                        if (state.statuscheck && state.statuscheck.sendWorkflowWebhook) {
                            if (state.statuscheck.sendWorkflowWebhook === 'always') {
                                await Stats.updateQueueStats({ queue: state.queue, status: 'webhook', }, IndexSchema, socketService)
                                await startFunctions.sendWebhook()
                            } else if  (state.statuscheck.sendWorkflowWebhook === 'onError') {
                                await Stats.updateQueueStats({ queue: state.queue, status: 'webhook', }, IndexSchema, socketService)
                                await startFunctions.sendWebhook()
                            }
                        } else {
                            await Stats.updateQueueStats({ queue: state.queue, status: 'webhook', }, IndexSchema, socketService)
                            await startFunctions.sendWebhook()
                        }
                    }
                } else {
                    await Stats.updateQueueStats({ queue: state.queue, status: 'error', statusText: err.message }, IndexSchema, socketService)
                    return snapshot
                }
            }
        }

    },
}