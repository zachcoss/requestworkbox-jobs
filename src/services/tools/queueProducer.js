const 
    _ = require('lodash'),
    mongoose = require('mongoose'),
    moment = require('moment'),
    IndexSchema = require('../tools/schema').schema,
    AWS = require('aws-sdk'),
    SQS = require('./sqs').SQS;

module.exports = {
    findQueueDocs: async (range) => {
        const findPayload = { status: 'received', queueType: { $in:  ['queue','schedule'] } }
        const projection = '_id'

        if (range === 'old') {
            findPayload['date'] = { $gt: moment().subtract(5, 'minutes'), $lt: moment(), }
        } else if (range === 'late') {
            findPayload['date'] = { $gt: moment().subtract(1, 'minutes'), $lt: moment(), }
        } else if (range === 'current') {
            findPayload['date'] = { $gt: moment().subtract(30, 'seconds'), $lt: moment().add(30, 'seconds'), }
        }

        try {
            const findQueueStart = new Date()
            const queueDocs = await IndexSchema.Queue.find(findPayload, projection).lean()
            await module.exports.sendQueueDocs(queueDocs)

            console.log(`completed ${_.size(queueDocs)} docs in ${new Date() - findQueueStart} ms`)
        } catch(err) {
            console.log('find err', err)
        }
    },
    sendQueueDocs: async (queueDocs) => {
        if (!_.size(queueDocs)) {
            console.log('no docs')
            return;
        }

        // Map and batch queueDocs
        const sendQueueDocs = _.map(queueDocs, (queueDoc) => {
            const queueDocId = queueDoc._id.toString()
            return {
                Id: queueDocId,
                MessageBody: queueDocId,
            }
        })
        const sendQueueDocsBatched = _.chunk(sendQueueDocs, 10)

        // Batch send and update
        for (sendQueueDocsBatch of sendQueueDocsBatched) {
            try {
                const sendQueueStart = new Date()
                const sendQueueBatch = await SQS.sendMessageBatch({
                    QueueUrl: process.env.AWS_QUEUE_STANDARD_URL,
                    Entries: sendQueueDocsBatch,
                }).promise()

                console.log(`sent in ${new Date() - sendQueueStart} ms`)

                await module.exports.updateQueueDocs(sendQueueBatch)
            } catch(err) {
                console.log('batch err', err)
            }
        }
    },
    updateQueueDocs: async (sendQueueBatch) => {
        if (!_.size(sendQueueBatch.Successful)) {
            console.log('no successful')
            return;
        }
        
        try {
            const updateQueueStart = new Date()
            const successfulDocIds = _.map(sendQueueBatch.Successful, 'Id')
            const successfulQuery = { _id: { $in: successfulDocIds } }
            const update = { status: 'queued' }
            const successfulDocs = await IndexSchema.Queue.updateMany(successfulQuery, update)

            console.log(`updated in ${new Date() - updateQueueStart} ms`)
        } catch(err) {
            console.log('update err', err)
        }
    },
}