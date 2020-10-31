const
    _ = require('lodash'),
    mongoose = require('mongoose'),
    moment = require('moment'),
    IndexSchema = require('../tools/schema').schema,
    Stats = require('../tools/stats'),
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
            const findPayload = { status: 'pending', _id: req.query.queueid }
            const queue = await IndexSchema.Queue.findOne(findPayload)

            if (!queue || !queue._id) {
                console.log('Queue not found')
                return res.status(500).send('Queue not found')
            }

            // Create Queue Pending Stat
            await Stats.updateQueueStats({ queue, status: 'queued', })

            // start immediately
            const workflowResult = await instanceTools.start(null, queue)
            return res.status(200).send(workflowResult)
        } catch (err) {
            console.log(err)
            return res.status(500).send(err)
        }
    },
}