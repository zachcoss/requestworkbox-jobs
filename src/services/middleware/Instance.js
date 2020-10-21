const
    _ = require('lodash'),
    mongoose = require('mongoose'),
    moment = require('moment'),
    IndexSchema = require('@requestworkbox/schema'),
    instanceTools = require('../tools/instance');

module.exports = {
    returnWorkflow: async (req, res, next) => {
        try {
            // filter query
            console.log('Request query ', req.query)
            if (!req.query || !req.query.queueid) {
                console.log('Missing queue id')
                return res.status(500).send('Missing queue id')
            }

            // find queue
            const findPayload = { status: 'received', _id: req.query.queueid }
            const projection = '_id'
            const queueDoc = await IndexSchema.Queue.findOne(findPayload, projection).lean()

            if (!queueDoc || !queueDoc._id) {
                console.log('Queue not found')
                return res.status(500).send('Queue not found')
            }

            // start immediately
            const workflowResult = await instanceTools.start(null, queueDoc)
            return res.status(200).send(workflowResult)
        } catch (err) {
            console.log(err)
            return res.status(500).send(err)
        }
    },
}