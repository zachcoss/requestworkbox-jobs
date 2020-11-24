const
    _ = require('lodash'),
    IndexSchema = require('../tools/schema').schema,
    passwordHash = require('pbkdf2-password-hash');

module.exports = {
    healthcheck: async function (req, res, next) {
        try {
            return res.status(200).send('OK')
        } catch (err) {
            return res.status(500).send('ERROR')
        }
    },
    interceptor: async function (req, res, next) {
        try {
            if ((!req.user || !req.user.sub) && !req.headers['x-api-key']) {
                return res.status(401).send('user not found')
            } else if (req.headers['x-api-key']) {

                // Only accept requests from api
                const referer = req.headers['referer'].split('/return-workflow')[0]
                if (!_.includes(referer, 'http://localhost:3000') || !_.includes(referer, 'https://api.requestworkbox.com')) {
                    return res.status(401).send('Incorrect referer')
                }
                
                const uuid = req.headers['x-api-key']
                const snippet = uuid.substring(0,8)
                const token = await IndexSchema.Token.findOne({ snippet, active: true, })
                const validToken = await passwordHash.compare(uuid, token.hash)
                
                req.user = { sub: token.sub }
                return next()
            } else {
                console.log('current user: ', req.user.sub)
                return next()
            }
        } catch (err) {
            console.log(err)
            return res.status(500).send('error intercepting user')
        }
    },
}