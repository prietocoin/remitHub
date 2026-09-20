const { Queue } = require('bullmq');
const redisConfig = require('../config/redis');

const colaEnsamblador = new Queue('cola-ensamblador', {
  connection: redisConfig,
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: 'exponential', delay: 2000 },
    removeOnComplete: true,
  }
});

module.exports = colaEnsamblador;
