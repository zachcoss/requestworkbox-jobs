const
    _ = require('lodash'),
    socketService = require('../tools/socket'),
    IndexSchema = require('../tools/schema').schema,
    Stats = require('../tools/stats').stats,
    instanceTools = require('../tools/instance');

module.exports = {
    returnWorkflow: async (req, res, next) => {
        try {
            // filter query
            if (!req.query || !req.query.queueid) {
                return res.status(400).send('Missing queue id.')
            }

            // find queue
            const findPayload = { _id: req.query.queueid, sub: req.user.sub, status: 'pending', }
            const queue = await IndexSchema.Queue.findOne(findPayload)

            if (!queue || !queue._id) {
                return res.status(400).send('Queue not found.')
            }

            // Create Queue Pending Stat
            await Stats.updateQueueStats({ queue, status: 'queued', }, IndexSchema, socketService)

            // start immediately
            const workflowResult = await instanceTools.start(null, queue)

            const publicUserObject = _.pick(queue, ['publicUser'])

            if (publicUserObject && publicUserObject['publicUser'] && publicUserObject['publicUser'] === false) {
                return res.status(200).send(workflowResult)
            }

            return res.sendStatus(200)
        } catch (err) {
            console.log('Return Workflow Error', err)
            return res.status(500).send('Return workflow error')
        }
    },
}