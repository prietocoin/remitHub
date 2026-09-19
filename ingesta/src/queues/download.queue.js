const { Queue } = require('bullmq');
const redisConfig = require('../config/redis');

const downloadQueue = new Queue('cola-descarga-media', {
  connection: redisConfig,
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: 'exponential', delay: 2000 },
    removeOnComplete: true,
  }
});

module.exports = downloadQueue;
