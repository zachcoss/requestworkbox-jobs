const 
    AWS = require('aws-sdk'),
    SQS = new AWS.SQS({ region: 'us-east-1' });

module.exports = {
    SQS: SQS
}