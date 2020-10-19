const 
    cron = require('cron'),
    CronJob = cron.CronJob,
    queueProducer = require('./queueProducer'),
    queueConsumer = require('./queueConsumer');

module.exports.init = () => {
    console.log('initializing jobs')

    // Queue Jobs
    // queueStandardOld.start()
    // queueStandardLate.start()
    // const queueStandardOld = new CronJob('0 */5 * * * *', function() {
    //     // console.log('You will see this message every five minutes')
    //     queueProducer.findQueueDocs('old')
    // })
    // const queueStandardLate = new CronJob('0 */1 * * * *', function() {
    //     // console.log('You will see this message every minute')
    //     queueProducer.findQueueDocs('late')
    // })
    const find = new CronJob('*/20 * * * * *', function() {
        // console.log('You will see this message every twenty seconds')
        queueProducer.findQueueDocs('current')
    }, null, true)

    const consume = new CronJob('*/20 * * * * *', function() {
        // console.log('You will see this message every twenty seconds')
        queueConsumer.receiveMessages()
    }, null, true)

    find.start()
    consume.start()

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