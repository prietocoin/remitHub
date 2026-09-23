const { Queue } = require('bullmq');
const { createBullBoard } = require('@bull-board/api');
const { BullMQAdapter } = require('@bull-board/api/bullMQAdapter');
const { ExpressAdapter } = require('@bull-board/express');
const redisConfig = require('../../../config/redis');

const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath('/admin/queues');

// Cola unificada principal de remitHub
const colaPipeline = new Queue('cola-pipeline', { connection: redisConfig });

createBullBoard({
  queues: [
    new BullMQAdapter(colaPipeline),
  ],
  serverAdapter: serverAdapter,
});

module.exports = serverAdapter.getRouter();
