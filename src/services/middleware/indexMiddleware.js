const
    _ = require('lodash'),
    IndexSchema = require('../tools/schema').schema,
    Tokens = require('../tools/tokens').tokens;

module.exports = {
    healthcheck: async function (req, res, next) {
        try {
            return res.status(200).send('OK')
        } catch (err) {
            return res.status(500).send('ERROR')
        }
    },
    validateOrigin: async function (req, res, next) {
        try {
            const origin = `${req.protocol}://${req.hostname}`
            const allowOrigin = process.env.NODE_ENV === 'production' ? 'https://api.requestworkbox.com' : 'http://localhost'
            
            if (origin !== allowOrigin) throw new Error()
            else return next()
        } catch (err) {
            console.log(err)
            return res.status(500).send('error validating origin user')
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
                return res.status(401).send('Authorization not found.')
            }
        } catch (err) {
            console.log('Interceptor error', err)
            return res.status(401).send('Token not found.')
        }
    },
}