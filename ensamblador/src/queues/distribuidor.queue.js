const { Queue } = require('bullmq');
const redisConfig = require('../config/redis');

const colaDistribuidor = new Queue('cola-distribuidor', {
  connection: redisConfig,
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: 'exponential', delay: 1000 },
    removeOnComplete: true,
  }
});

module.exports = colaDistribuidor;
