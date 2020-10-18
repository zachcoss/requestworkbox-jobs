const 
    _ = require('lodash'),
    mongoose = require('mongoose'),
    moment = require('moment'),
    IndexSchema = require('../schema/indexSchema'),
    AWS = require('aws-sdk'),
    QueueStandard = new AWS.SQS({ region: 'us-east-1' });

module.exports = {
    
}