const
    _ = require('lodash'),
    mongoose = require('mongoose'),
    moment = require('moment'),
    socketService = require('../tools/socket'),
    IndexSchema = require('../tools/schema').schema,
    Stats = require('../tools/stats').stats,
    instanceTools = require('../tools/instance');

module.exports = {
    returnWorkflow: async (req, res, next) => {
        try {
            // filter query
            if (!req.query || !req.query.queueid) {
                console.log('Missing queue id')
                return res.status(500).send('Missing queue id')
            }

            // find queue
            const findPayload = { _id: req.query.queueid, sub: req.user.sub, status: 'pending', }
            const queue = await IndexSchema.Queue.findOne(findPayload)

            if (!queue || !queue._id) {
                console.log('Queue not found')
                return res.status(500).send('Queue not found')
            }

            // Create Queue Pending Stat
            await Stats.updateQueueStats({ queue, status: 'queued', }, IndexSchema, socketService)

            // start immediately
            const workflowResult = await instanceTools.start(null, queue)
            return res.status(200).send(workflowResult)
        } catch (err) {
            console.log(err)
            return res.status(500).send(err)
        }
    },
}