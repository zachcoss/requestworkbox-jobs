const 
    _ = require('lodash'),
    mongoose = require('mongoose'),
    moment = require('moment'),
    IndexSchema = require('../tools/schema').schema,
    AWS = require('aws-sdk'),
    instanceTools = require('./instance'),
    SQS = require('./sqs').SQS;

module.exports = {
    receiveMessages: async () => {
        try {
            const messagesStart = new Date()
            const receiveResponse = await SQS.receiveMessage({
                QueueUrl: process.env.AWS_QUEUE_STANDARD_URL,
                MaxNumberOfMessages: 10,
                WaitTimeSeconds: 5,
                VisibilityTimeout: 10,
            }).promise()

            console.log(`received ${_.size(receiveResponse.Messages)} messages in ${new Date() - messagesStart} seconds`)
            
            if (!_.size(receiveResponse.Messages)) {
                console.log('no messages')
                return;
            }

            for (queuePayload of receiveResponse.Messages) {
                instanceTools.start(queuePayload)
            }
        } catch(err) {
            console.log('receive messages err ', err)
        }
    },
}