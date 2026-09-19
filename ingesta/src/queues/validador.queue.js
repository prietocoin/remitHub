const { Queue } = require('bullmq');
const redisConfig = require('../config/redis');

const validadorQueue = new Queue('cola-validador', {
  connection: redisConfig,
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: 'exponential', delay: 1000 },
    removeOnComplete: true,
  }
});

module.exports = validadorQueue;
