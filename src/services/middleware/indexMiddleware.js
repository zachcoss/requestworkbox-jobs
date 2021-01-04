const
    _ = require('lodash'),
    moment = require('moment'),
    IndexSchema = require('../tools/schema').schema,
    Tokens = require('../tools/tokens').tokens;

let 
    api = {},
    apiEnd = moment(),

    workflow = {},
    workflowEnd = moment(),

    workflowPaths = ['/return-workflow'];

module.exports = {
    ratelimit: async function (req, res, next) {
        try {

            let ipAddress;

            if (req.hostname === 'localhost') ipAddress = 'localhost'
            else ipAddress = req.ip

            if (_.includes(workflowPaths, req.path)) {
                if (moment().isAfter(workflowEnd)) {
                    workflow = {}
                    workflowEnd = moment().add(60, 'second')
                }

                if (!workflow[ipAddress]) workflow[ipAddress] = 1
                else workflow[ipAddress] = workflow[ipAddress] + 1

                if (workflow[ipAddress] <= 1000) return next()

                console.log('Rate limiting Workflow: ', ipAddress)
                return res.sendStatus(429)
            } else {
                if (moment().isAfter(apiEnd)) {
                    api = {}
                    apiEnd = moment().add(5, 'second')
                }

                if (!api[ipAddress]) api[ipAddress] = 1
                else api[ipAddress] = api[ipAddress] + 1

                if (api[ipAddress] <= 10) return next()

                console.log('Rate limiting API: ', ipAddress)
                return res.sendStatus(429)
            }
        } catch (err) {
            console.log('Rate limit error', err)
            return res.status(500).send('Rate limit error.')
        }
    },
    healthcheck: async function (req, res, next) {
        try {
            return res.status(200).send('OK')
        } catch (err) {
            return res.status(500).send('ERROR')
        }
    },
    interceptor: async function (req, res, next) {
        try {
            if (req.user && req.user.sub && _.isString(req.user.sub)) return next()
            else if (req.headers['x-api-key']) {
                const sub = await Tokens.validateToken(IndexSchema, req.headers['x-api-key'])
                req.user = { sub, }

                return next()
            } else {
                if (req.path === '/return-workflow') return next()
                return res.status(401).send('Authorization not found.')
            }
        } catch (err) {
            console.log('Interceptor error', err)
            return res.status(401).send('Token not found.')
        }
    },
}