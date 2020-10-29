const 
    cron = require('cron'),
    CronJob = cron.CronJob,
    queueProducer = require('./queueProducer'),
    queueConsumer = require('./queueConsumer');

module.exports.init = () => {
    console.log('initializing jobs')

    // Move from DB to Queue every 5 seconds
    const find = new CronJob('*/5 * * * * *', function() {
        queueProducer.findQueueDocs('current')
    }, null, true)

    // Pull from Queue every 5 seconds
    const consume = new CronJob('*/5 * * * * *', function() {
        queueConsumer.receiveMessages()
    }, null, true)

    // Initialize both jobs
    find.start()
    consume.start()
    
}