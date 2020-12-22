const 
    _ = require('lodash'),
    mongoose = require('mongoose'),
    moment = require('moment'),
    socketService = require('./socket'),
    IndexSchema = require('../tools/schema').schema,
    AWS = require('aws-sdk'),
    Stats = require('../tools/stats').stats,
    SQS = require('./sqs').SQS;

module.exports = {
    findQueueDocs: async (range) => {
        const findPayload = { status: 'pending', queueType: { $in:  ['queue','schedule'] } }

        if (range === 'old') {
            findPayload['date'] = { $gt: moment().subtract(5, 'minutes'), $lt: moment(), }
        } else if (range === 'late') {
            findPayload['date'] = { $gt: moment().subtract(1, 'minutes'), $lt: moment(), }
        } else if (range === 'current') {
            findPayload['date'] = { $gt: moment().subtract(30, 'seconds'), $lt: moment().add(30, 'seconds'), }
        }

        try {
            const queueDocs = await IndexSchema.Queue.find(findPayload)
            await module.exports.sendQueueDocs(queueDocs)
        } catch(err) {
            console.log('find err', err)
        }
    },
    sendQueueDocs: async (queueDocs) => {
        if (!_.size(queueDocs)) {
            console.log('no docs')
            return;
        }

        // Create Queue Updates/Stats
        await Stats.batchUpdateQueueStats({ queueDocs, status: 'queued' }, IndexSchema, socketService)

        // Map and batch Queue Docs
        const sendQueueDocs = _.map(queueDocs, (queueDoc) => {
            const queueDocId = queueDoc._id.toString()
            return {
                Id: queueDocId,
                MessageBody: `${queueDocId} ${moment(queueDoc.date).toISOString()}`,
            }
        })
        const sendQueueDocsBatched = _.chunk(sendQueueDocs, 10)

        // Batch send and update
        for (sendQueueDocsBatch of sendQueueDocsBatched) {
            try {
                const sendQueueBatch = await SQS.sendMessageBatch({
                    QueueUrl: process.env.AWS_QUEUE_STANDARD_URL,
                    Entries: sendQueueDocsBatch,
                }).promise()
            } catch(err) {
                console.log('sendQueueDocs batch err', err)
            }
        }
    },
}