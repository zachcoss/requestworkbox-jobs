const 
    cron = require('cron'),
    CronJob = cron.CronJob,
    ProducerQueue = require('./producerQueue');

module.exports.init = () => {
    console.log('initializing jobs')

    // Queue Jobs
    const queueStandardOld = new CronJob('0 */5 * * * *', function() {
        console.log('You will see this message every five minutes')
        ProducerQueue.findQueueDocs('old')
    })
    const queueStandardLate = new CronJob('0 */1 * * * *', function() {
        console.log('You will see this message every minute')
        ProducerQueue.findQueueDocs('late')
    })
    const queueStandardCurrent = new CronJob('*/20 * * * * *', function() {
        console.log('You will see this message every twenty seconds')
        ProducerQueue.findQueueDocs('current')
    })

    queueStandardOld.start()
    queueStandardLate.start()
    queueStandardCurrent.start()

    // const job = new CronJob('*/30 * * * * *', function() {
    //     console.log('You will see this message every thirty seconds')
    // })
    // const job = new CronJob('*/10 * * * * *', function() {
    //     console.log('You will see this message every ten seconds')
    // })
    // const job = new CronJob('* * * * * *', function() {
    //     console.log('You will see this message every second')
    // })
    
}