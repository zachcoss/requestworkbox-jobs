const 
    _ = require('lodash'),
    mongoose = require('mongoose'),
    moment = require('moment'),
    IndexSchema = require('../schema/indexSchema'),
    AWS = require('aws-sdk'),
    QueueStandard = new AWS.SQS({ region: 'us-east-1' });

module.exports = {
    findQueueDocs: async (range) => {
        const findPayload = { status: 'received' }
        const projection = '_id'

        if (range === 'old') {
            findPayload['date'] = { $gt: moment().subtract(5, 'minutes'), $lt: moment(), }
        } else if (range === 'late') {
            findPayload['date'] = { $gt: moment().subtract(1, 'minutes'), $lt: moment(), }
        } else if (range === 'current') {
            findPayload['date'] = { $gt: moment().subtract(20, 'seconds'), $lt: moment().add(20, 'seconds'), }
        }

        try {
            const queueDocs = await IndexSchema.Queue.find(findPayload, projection).lean()
            await module.exports.sendQueueDocs(queueDocs)
        } catch(err) {
            console.log('err', err)
        }
    },
    sendQueueDocs: async (queueDocs) => {
        for (const queueDoc of queueDocs) {
            try {
                const queueDocId = queueDoc._id.toString()
                await QueueStandard.sendMessage({
                    MessageBody: queueDocId,
                    QueueUrl: process.env.AWS_QUEUE_STANDARD_URL,
                }).promise()
                console.log('sent ', queueDocId)
                await module.exports.updateQueueDoc(queueDocId)
            } catch(err) {
                console.log('err', err)
            }
        }
    },
    updateQueueDoc: async (queueDocId) => {
        try {
            await IndexSchema.Queue.findOneAndUpdate(queueDocId, { status: 'queued' })
            console.log('updated ', queueDocId)
        } catch(err) {
            console.log('err', err)
        }
    },
}