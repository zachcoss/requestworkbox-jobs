# Request Workbox Jobs

## Description

This is a backend Jobs API based on the Express CLI generator. The Jobs API works in conjunction with the Request Workbox API, by queuing and initializing workflow jobs created by the API. The queuing functionality is managed by AWS SQS queues and workflows are run via Axios instances.

## Location

The Jobs API is available at https://jobs.requestworkbox.com.

## Libraries

- AWS-SDK
- Axios
- Express
- Express-JWT
- JWKS-RSA
- Lodash
- Moment
- Mongoose
- Socket io

## Details

Request Workbox was built and managed by Zach Coss. Request Workbox was created to provide tooling for REST APIs, such as managing parameters, permission based responses, team access, and options for running and monitoring multiple requests and workflows concurrently (with queuing and scheduling).