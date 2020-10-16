const
    _ = require('lodash');

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
            if (!req.user || !req.user.sub) {
                return res.status(500).send('user not found')
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