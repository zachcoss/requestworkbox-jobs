const 
    mongoose = require('mongoose'),
    mongooseAutopopulate = require('mongoose-autopopulate'),
    schema = require('@requestworkbox/internal-tools').schema;

module.exports = {
    schema: schema(mongoose, mongooseAutopopulate, process.env.NODE_ENV)
}