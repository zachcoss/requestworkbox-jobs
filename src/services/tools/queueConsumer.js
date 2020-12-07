const 
    _ = require('lodash'),
    cron = require('cron'),
    CronJob = cron.CronJob,
    mongoose = require('mongoose'),
    moment = require('moment'),
    IndexSchema = require('../tools/schema').schema,
    AWS = require('aws-sdk'),
    instanceTools = require('./instance'),
    SQS = require('./sqs').SQS;

module.exports = {
    existingMessages: {},
    receiveMessages: async () => {
        try {
            const messagesStart = new Date()
            const receiveResponse = await SQS.receiveMessage({
                QueueUrl: process.env.AWS_QUEUE_STANDARD_URL,
                MaxNumberOfMessages: 10,
                WaitTimeSeconds: 5,
                VisibilityTimeout: 5,
            }).promise()

            console.log(`received ${_.size(receiveResponse.Messages)} messages in ${new Date() - messagesStart} seconds`)
            
            if (!_.size(receiveResponse.Messages)) {
                console.log('no messages')
                return;
            }

            // run payloads
            for (queuePayload of receiveResponse.Messages) {
                const queuePayloadSplit = queuePayload.Body.split(' ')
                const queueId = queuePayloadSplit[0]
                const queueDate = moment(queuePayloadSplit[1])

                if (!module.exports.existingMessages[queueId]) {
                    module.exports.existingMessages[queueId] = queueDate

                    if (queueDate.isAfter(moment().add(1, 'second'))) {
                        const job = new CronJob(queueDate.toDate(), function() {
                            instanceTools.start(queuePayload)
                        }, null)
                        job.start()
                    } else {
                        instanceTools.start(queuePayload)
                    }
                } else {
                    // extend
                    if (queueDate.isAfter(moment().subtract(30, 'seconds'))) {
                        instanceTools.start(queuePayload, null, 'extend')
                    } else {
                        instanceTools.start(queuePayload)
                    }
                }
            }

            // delete old existing messages
            module.exports.existingMessages = _.pickBy(module.exports.existingMessages, function(existingMessageDate, existingMessageQueueId) {
                if (existingMessageDate.isAfter(moment().subtract(1, 'minute'))) return true
                else return false
            })
        } catch(err) {
            console.log('receive messages err ', err)
        }
    },
}