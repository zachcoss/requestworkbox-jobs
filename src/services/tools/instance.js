const { over } = require('lodash');

const
    _ = require('lodash'),
    moment = require('moment'),
    Axios = require('axios'),
    IndexSchema = require('../tools/schema').schema,
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
    SQS = require('./sqs').SQS,
    validUrl = require('valid-url'),
    productionUrls = [
        'https://dashboard.requestworkbox.com', 'https://dashboard.requestworkbox.com/',
        'https://api.requestworkbox.com', 'https://api.requestworkbox.com/',
        'https://jobs.requestworkbox.com', 'https://jobs.requestworkbox.com/',
        'https://billing.requestworkbox.com', 'https://billing.requestworkbox.com/',
        'https://support.requestworkbox.com', 'https://support.requestworkbox.com/',
        'https://service.requestworkbox.com', 'https://service.requestworkbox.com/',
        'https://primitives.requestworkbox.com', 'https://primitives.requestworkbox.com/',],
    stagingUrls = [
        'http://localhost:8080', 'http://localhost:8080/', // dashboard/support
        'http://localhost:3000', 'http://localhost:3000/', // api
        'http://localhost:3001', 'http://localhost:3001/', // billing
        'http://localhost:3002', 'http://localhost:3002/', // support-api
        'http://localhost:4000', 'http://localhost:4000/', // jobs
        'http://localhost:5000', 'http://localhost:5000/',]; // primitives

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

        let snapshot = {
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
            runtimeResultNames: {},
            webhookRequestId: null,
            webhookTaskId: null,
        }

        const queueFunctions = {
            getQueue: async function() {
                const queue = await IndexSchema.Queue.findOne({ _id: incoming.queueId, })
                if (!queue || !queue._id) throw new Error('Queue not found.')

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
            getBodyPayload: async function() {
                if (!state.workflow.payloads[0].active) return;

                if (state.queue.storageInstanceId && state.queue.storageInstanceId !== '') {
                    const payloadTaskId = state.workflow.payloads[0]._id
                    // pull body payload
                    const bodyPayloadStart = new Date()
                    const storageValue = await S3.getObject({
                        Bucket: process.env.STORAGE_BUCKET,
                        Key: `${state.workflow.projectId}/workflow-payloads/${state.queue.storageInstanceId}`,
                    }).promise()

                    // add payload to snapshot
                    processFunctions.processRequestResults(JSON.parse(storageValue.Body), payloadTaskId, 'payloads')

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
                const instance = await IndexSchema.Instance.findOne({ _id: state.queue.instanceId, sub: state.queue.sub })
                if (!instance || !instance._id) throw new Error('Instance not found.')

                state.instance = instance
            },
            getWorkflow: async function() {
                const workflow = await IndexSchema.Workflow.findOne({ _id: state.instance.workflowId, projectId: state.instance.projectId }).lean()
                if (!workflow || !workflow._id) throw new Error('Workflow not found.')

                if (workflow && _.isBoolean(workflow.preventExecution) && workflow.preventExecution) throw new Error('Preventing workflow execution.')

                state.workflow = workflow

                _.each(state.workflow.tasks, (task) => {
                    if (task.runtimeResultName && task.runtimeResultName !== '') {
                        state.runtimeResultNames[task.runtimeResultName] = task._id
                    }
                })
            },
            getRequests: async function() {
                try {
                    for (task of state.workflow.tasks) {
    
                        if (!task.requestId || task.requestId === '') return
                        if (state.requests[task.requestId]) return
                        if (!task.active) return
    
                        const request = await IndexSchema.Request.findOne({ _id: task.requestId, projectId: state.instance.projectId, }).lean()
                        if (!request || !request._id) throw new Error('Request not found.')

                        if (request && _.isBoolean(request.preventExecution) && request.preventExecution) throw new Error('Preventing request execution.')
    
                        if (!request.url) return callback('Missing URL.')
                        if (!/^https?:\/\//.test(request.url)) throw new Error('Must be secure URL.')
                        if (!validUrl.isWebUri(request.url)) throw new Error('Not valid URL.')
                        if (_.includes(request.url, 'requestworkbox.com')) {
                            if (!_.includes(productionUrls, request.url)) throw new Error('Recursive request URLs not allowed.')
                        }

                        // find storages
                        await getFunctions.getStorages(request)
    
                        state.requests[task.requestId] = request
                    }

                    for (webhook of state.workflow.webhooks) {
                        if (!webhook.requestId || webhook.requestId === '') return;
                        if (state.requests[webhook.requestId]) return;
                        if (!task.active) return;

                        const request = await IndexSchema.Request.findOne({ _id: webhook.requestId, projectId: state.instance.projectId }).lean()
                        if (!request || !request._id) throw new Error('Webhook not found.')

                        if (request && _.isBoolean(request.preventExecution) && request.preventExecution) throw new Error('Preventing webhook execution.')

                        if (!request.url) throw new Error('Missing URL.')
                        if (!/^https?:\/\//.test(request.url)) throw new Error('Must be secure URL.')
                        if (!validUrl.isWebUri(request.url)) throw new Error('Not valid URL.')
                        if (_.includes(request.url, 'requestworkbox.com')) {
                            if (!_.includes(productionUrls), request.url) throw new Error('Recursive webhook URLs not allowed.')
                        }

                        state.requests[webhook.requestId] = request
                        state.webhookRequestId = webhook.requestId
                        state.webhookTaskId = webhook._id
                    }
                } catch(err) {
                    console.log('Get requests error', err)
                    throw new Error(`Get requests error: ${err.message}`)
                }
            },
            storageRequest: async function(obj) {
                try {
                    if (obj.valueType !== 'storage') return;
                    if (state.storages[obj.value]) return;

                    const storage = await IndexSchema.Storage.findOne({ _id: obj.value, projectId: state.instance.projectId }).lean()
                    if (!storage || !storage._id) throw new Error('Storage not found.')

                    if (storage && _.isBoolean(storage.preventExecution) && storage.preventExecution) throw new Error('Preventing storage execution.')

                    await getFunctions.getStorageDetails(storage)

                    state.storages[obj.value] = storage
                } catch(err) {
                    console.log('Storage request error', err)
                    throw new Error(`Storage request error: ${err.message}`)
                }
            },
            getStorages: async function(request) {
                try {
                    for (const obj of request.authorization) {
                        await getFunctions.storageRequest(obj)
                    }
                    for (const obj of request.query) {
                        await getFunctions.storageRequest(obj)
                    }
                    for (const obj of request.headers) {
                        await getFunctions.storageRequest(obj)
                    }
                    for (const obj of request.body) {
                        await getFunctions.storageRequest(obj)
                    }
                } catch(err) {
                    console.log('Get storages error', err)
                    throw new Error(`Get storages error: ${err.message}`)
                }
            },
            getStorageDetails: async function(storage) {
                try {
                    const storageValueStart = new Date()
                    const storageValue = await S3.getObject({
                        Bucket: process.env.STORAGE_BUCKET,
                        Key: `${state.instance.projectId}/storage-data/${storage._id}`,
                    }).promise()

                    const storageValueSize = storageValue.Body.byteLength

                    if (storage.storageType === 'text') {
                        storage.storageValue = String(storageValue.Body)
                    } else if (storage.storageType === 'file') {
                        try {
                            storage.storageValue = JSON.parse(storageValue.Body)
                        } catch(err) {
                            storage.storageValue = String(storageValue.Body)
                        }
                    }

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
                } catch(err) {
                    console.log('Get storage details error', err)
                    throw new Error(`Get storage details error: ${err.message}`)
                }
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
                    authorization: {},
                    query: {},
                    headers: {},
                    body: {},
                    sensitiveResponse: false,
                }

                const request = state.requests[requestId]
                if (request && _.isBoolean(request.sensitiveResponse) && request.sensitiveResponse) requestTemplate.sensitiveResponse = true

                const requestDetails = _.pick(request, ['authorization','query','headers','body'])

                requestTemplate.url = request.url
                requestTemplate.name = request.name
                requestTemplate.method = request.method

                // Apply inputs
                _.each(requestDetails, (requestDetailArray, requestDetailKey) => {
                    _.each(requestDetailArray, (requestDetailObj) => {
                        if (!requestDetailObj.active) return
                        if (requestDetailObj.key === '' || requestDetailObj.value === '') return
                        
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
                                if (taskObj.taskId === taskId && taskObj.response) {
                                    requestTemplate[requestDetailKey][requestDetailObj.key] = taskObj.response
                                }
                            })
                        } else if (requestDetailObj.valueType === 'incomingField') {
                            const incomingFieldName = requestDetailObj.value
                            _.each(snapshot.payloads, (payloadObj) => {
                                if (payloadObj.response && payloadObj.response[incomingFieldName]) {
                                    requestTemplate[requestDetailKey][requestDetailObj.key] = payloadObj.response[incomingFieldName]
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

                    timeout: 30000,
                    maxContentLength: 5000000,
                    maxBodyLength: 5000000,
                    maxRedirects: 0,
                }

                if (requestTemplate.authorization.username && requestTemplate.authorization.password) {
                    if (!_.isString(requestTemplate.authorization.username)) return;
                    if (!_.isString(requestTemplate.authorization.password)) return;
                    
                    requestConfig.auth = {
                        username: requestTemplate.authorization.username,
                        password: requestTemplate.authorization.password,
                    }
                }
                
                const statConfig = {
                    instanceId: state.instance._id,
                    requestName: requestTemplate.name,
                    requestType: requestType,
                    requestId: requestTemplate.requestId,
                    taskId: requestTemplate.taskId,
                    taskField: requestTemplate.taskField,
                    requestPayload: _.omit(requestConfig, ['timeout','maxContentLength','maxBodyLength','maxRedirects']),
                    responsePayload: {},
                    status: 0,
                    statusText: '',
                    startTime: new Date(),
                    endTime: new Date(),
                    sensitiveResponse: requestTemplate.sensitiveResponse,
                }
                
                try {

                    const requestLengthSize = Buffer.byteLength(JSON.stringify(requestConfig), 'utf8')
                    if (requestLengthSize > 5000000) throw new Error('Request size error: 5MB max request limit.')

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

                    let requestResults = _.pick(request, ['data', 'status', 'statusText','headers'])

                    const resultLengthSize = Buffer.byteLength(JSON.stringify(requestResults), 'utf8')
                    if (resultLengthSize > 5000000) throw new Error('Response size error: 5MB max response limit.')

                    requestResults.requestName = statConfig.requestName
                    requestResults.sensitiveResponse = statConfig.sensitiveResponse

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

                    throw new Error(err)

                    // Use for "Workflow Continue After Error"
                    // if (onWorkflowTaskError && onWorkflowTaskError === 'continue') {
                    //     console.log('continue after error')
                    // } else {
                    //     throw new Error(err)
                    // }
                }
            },
        }

        const statFunctions = {
            createStat: async function(statConfig) {
                try {
                    await Stats.updateInstanceStats({ instance: state.instance, statConfig, }, IndexSchema, S3, process.env.STORAGE_BUCKET, socketService)
                } catch(err) {
                    console.log('create stat error', err)
                    throw new Error('Error creating stat')
                }
            }
        }

        const processFunctions = {
            processRequestResults: async function(requestResults, taskId, taskField) {
                const over1MB = Buffer.byteLength(JSON.stringify(requestResults)) > 1000000 ? true : false

                let data = {
                    taskId: taskId,
                    requestName: requestResults.requestName || '',
                    status: requestResults.status,
                }

                if (requestResults && _.isBoolean(requestResults.sensitiveResponse) && requestResults.sensitiveResponse) data.sensitiveResponse = true

                if (taskField === 'payloads') {
                    if (!requestResults) {
                        data.error = true
                        data.errorMessage = 'Response error: missing data.'
                    }

                    if (over1MB) {
                        data.error = true
                        data.errorMessage = 'Response snapshot error: 1MB max response limit.'
                    } else {
                        data.response = requestResults
                    }
                }

                if (taskField === 'webhooks' || taskField === 'tasks') {
                    if (!requestResults.data) {
                        data.error = true
                        data.errorMessage = 'Response error: missing data.'
                    }   
                    if (over1MB) {
                        data.error = true
                        data.errorMessage = 'Response snapshot error: 1MB max response limit.'
                    } else {
                        data.response = requestResults.data
                    }
                }

                snapshot[taskField].push(data)
            },
        }

        const startFunctions = {
            startWorkflow: async function() {
                for (const task of state.workflow.tasks) {
                    if (!task.active) return;
                    
                    const requestTemplate = await templateFunctions.templateInputs(task.requestId, task._id, 'tasks')
                    const requestResults = await runFunctions.runRequest(requestTemplate, 'request')
                    processFunctions.processRequestResults(requestResults, task._id, 'tasks')
                }
            },
            sendWebhook: async function() {
                if (!state.webhookRequestId || state.webhookRequestId === '') return
    
                const requestTemplate = await templateFunctions.templateInputs(state.webhookRequestId, state.webhookTaskId, 'webhooks')
                const requestResults = await runFunctions.runRequest(requestTemplate, 'webhook')
                processFunctions.processRequestResults(requestResults, state.webhookTaskId, 'webhooks')
            },
            sendSnapshot: async function() {
                const 
                    publicUserObject = _.pick(state.queue, ['publicUser']),
                    publicUser = publicUserObject['publicUser'];

                let finalSnapshot = {
                    tasks: [],
                }

                if (_.isBoolean(publicUser) && !publicUser) {

                    const member = await IndexSchema.Member.findOne({
                        sub: state.instance.sub,
                        projectId: state.instance.projectId,
                    }).lean()
                    if (!member || !member._id) throw new Error('Snapshot permissions error.')
                    if (!member.active) throw new Error('Snapshot permission error.')
                    if (member.status !== 'accepted') throw new Error('Snapshot permission error.')
                    if (member.permission === 'none') throw new Error('Snapshot permission error.')

                    finalSnapshot.queueId = state.queue._id
                    finalSnapshot.instanceId = state.instance._id

                    _.each(snapshot.payloads, (payload) => {
                        if (!finalSnapshot.payloads) finalSnapshot.payloads = []
                        finalSnapshot.payloads.push(_.pick(payload, ['response','error','errorMessage']))
                    })

                    _.each(snapshot.tasks, (task) => {
                        if (task.sensitiveResponse) {
                            if (member.permission === 'read' && !member.includeSensitive) return finalSnapshot.tasks.push(_.pick(task, ['taskId','requestName','status','error','errorMessage']))
                        }

                        finalSnapshot.tasks.push(_.pick(task, ['taskId','requestName','status','error', 'errorMessage', 'response']))
                    })

                    return finalSnapshot

                } else if (_.isBoolean(publicUser) && publicUser) {

                    _.each(snapshot.tasks, (task) => {
                        finalSnapshot.tasks.push(_.pick(task, ['requestName','status','error','errorMessage','response']))
                    })

                    return finalSnapshot
                }
                
            },
        }

        const init = async () => {

            // initialize queue state
            await queueFunctions.getQueue()
            await queueFunctions.processQueue()

            await Stats.updateQueueStats({ queue: state.queue, status: 'initializing', }, IndexSchema, socketService)
            
            // initialize instance state
            await getFunctions.getInstance()
            await getFunctions.getWorkflow()

            await Stats.updateQueueStats({ queue: state.queue, status: 'loading', }, IndexSchema, socketService)
            
            await getFunctions.getRequests()
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
                    await Stats.updateQueueStats({ queue: state.queue, status: 'webhook', }, IndexSchema, socketService)
                    await startFunctions.sendWebhook()
                }

                await Stats.updateQueueStats({ queue: state.queue, status: 'complete', }, IndexSchema, socketService)
            } else {
                await Stats.updateQueueStats({ queue: state.queue, status: 'complete', }, IndexSchema, socketService)
            }

        }

        try {
            // Initialize and set snapshot data
            await init()

            const finalSnapshot = await startFunctions.sendSnapshot()
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
                        await Stats.updateQueueStats({ queue: state.queue, status: 'webhook', }, IndexSchema, socketService)
                        await startFunctions.sendWebhook()
                    }
                } else {
                    await Stats.updateQueueStats({ queue: state.queue, status: 'error', statusText: err.message }, IndexSchema, socketService)
                    
                    const finalSnapshot = await startFunctions.sendSnapshot()
                    return finalSnapshot
                }
            }
        }

    },
}