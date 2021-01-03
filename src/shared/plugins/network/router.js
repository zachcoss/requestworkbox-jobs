const 
    express = require('express'),
    router = express.Router(),
    indexMiddleware = require('../../../services/middleware/indexMiddleware'),
    Instance = require('../../../services/middleware/Instance');

module.exports.config = function () {

    router.all('*', indexMiddleware.ratelimit)
    router.get('/', indexMiddleware.healthcheck)
    router.all('*', indexMiddleware.interceptor)

    router.get('/return-workflow', Instance.returnWorkflow)

    return router;
}