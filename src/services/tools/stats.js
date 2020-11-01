const
    _ = require('lodash'),
    socketService = require('./socket'),
    IndexSchema = require('./schema').schema,
    S3 = require('./s3').S3;

    module.exports = {
        updateQueueStats: async function(payload) {
            const { queue, status, statusText, error } = payload
            
            // Create Queue Stat
            const queueStat = new IndexSchema.QueueStat({
                active: true,
                sub: queue.sub,
                instance: queue.instance,
                queue: queue._id,
                status: status,
                statusText: statusText || '',
                error: error || false,
            })
            await queueStat.save()

            // Add to Queue
            queue.stats.push(queueStat)
            // Update status
            queue.status = status
            // Save queue
            await queue.save()

            socketService.io.emit(queue.sub, { queueDoc: queue, })
        },
        batchUpdateQueueStats: async function(payload) {
            const { queueDocs, status, statusText, error } = payload

            // Batch create queue stats
            const bulkQueueStats = _.map(queueDocs, (queue) => {
                // Create Queue Stat
                const queueStat = new IndexSchema.QueueStat({
                    active: true,
                    sub: queue.sub,
                    instance: queue.instance,
                    queue: queue._id,
                    status: status,
                    statusText: statusText || '',
                    error: error || false,
                })
                return queueStat
            })

            // Insert many queue stat
            const insertMany = await IndexSchema.QueueStat.insertMany(bulkQueueStats)

            // Batch update queue docs
            const bulkQueueUpdates = _.map(insertMany, (queueStat) => {
                return {
                    updateOne: {
                        filter: { _id: queueStat.queue },
                        update: {
                            $set : { status : queueStat.status },
                            $push: { stats: queueStat._id },
                        },
                    }
                }
            })

            // Bulk write queues
            const bulkWrite = await IndexSchema.Queue.bulkWrite(bulkQueueUpdates)

            // Generate artificial clone of queue doc for socket
            _.each(queueDocs, (queue) => {
                const queueStat = _.filter(bulkQueueStats, (queueStat) => {
                    if (queueStat.queue === queue._id) return true
                    else return false
                })[0]

                queue.stats.push(queueStat)
                queue.status = 'queued'
                socketService.io.emit(queue.sub, { queueDoc: queue, })
            })
        },
        updateInstanceStats: async function(payload) {
            const { instance, statConfig, err } = payload

            // Create Instance Stat
            const omittedStat = _.omit(statConfig, ['requestPayload','responsePayload', 'headers'])
            const instanceStat = new IndexSchema.Stat(omittedStat)
            await instanceStat.save()

            // Add to Instance
            instance.stats.push(instanceStat._id)
            await instance.save()

            // S3 Stat
            const statBackup = _.assign(statConfig, {_id: instanceStat._id})
            await S3.upload({
                Bucket: "connector-storage",
                Key: `${instance.sub}/instance-statistics/${statBackup.instance}/${statBackup._id}`,
                Body: JSON.stringify(statBackup)
            }).promise()
        },
    }