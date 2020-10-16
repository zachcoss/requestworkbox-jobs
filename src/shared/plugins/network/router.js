const 
    express = require('express'),
    router = express.Router(),
    indexMiddleware = require('../../../services/middleware/indexMiddleware'),
    Billing = require('../../../services/middleware/Billing');

module.exports.config = function () {

    router.get('/', indexMiddleware.healthcheck)
    router.all('*', indexMiddleware.interceptor)

    router.post('/get-account-type', Billing.getAccountType)
    router.post('/update-account-type', Billing.updateAccountType)

    return router;
}