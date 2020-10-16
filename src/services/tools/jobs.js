const 
    cron = require('cron');

module.exports.init = () => {
    console.log('initializing jobs')

    const CronJob = cron.CronJob
    // const job = new CronJob('0 */1 * * * *', function() {
    //     console.log('You will see this message every minute')
    // })
    // const job = new CronJob('*/30 * * * * *', function() {
    //     console.log('You will see this message every thirty seconds')
    // })
    const job = new CronJob('*/10 * * * * *', function() {
        console.log('You will see this message every ten seconds')
    })
    // const job = new CronJob('* * * * * *', function() {
    //     console.log('You will see this message every second')
    // })
    job.start()
}