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

            for (queuePayload of receiveResponse.Messages) {
                const queuePayloadSplit = queuePayload.Body.split(' ')
                const queueId = queuePayloadSplit[0]
                const queueDate = moment(queuePayloadSplit[1]).toDate()

                if (!module.exports.existingMessages[queueId]) {
                    module.exports.existingMessages[queueId] = queueDate
                    const job = new CronJob(queueDate, function() {
                        instanceTools.start(queuePayload)
                    }, null)
                    job.start()
                } else {
                    console.log('already in queue')
                }
            }
        } catch(err) {
            console.log('receive messages err ', err)
        }
    },
}