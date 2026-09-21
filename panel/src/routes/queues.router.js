const { Queue } = require('bullmq');
const { createBullBoard } = require('@bull-board/api');
const { BullMQAdapter } = require('@bull-board/api/bullMQAdapter');
const { ExpressAdapter } = require('@bull-board/express');
const redisConfig = require('../config/redis');

// Adaptador para Express
const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath('/admin/queues');

// Conexión a las 3 colas activas
const colaValidador = new Queue('cola-validador', { connection: redisConfig });
const colaEnsamblador = new Queue('cola-ensamblador', { connection: redisConfig });
const colaDistribuidor = new Queue('cola-distribuidor', { connection: redisConfig });

createBullBoard({
  queues: [
    new BullMQAdapter(colaValidador),
    new BullMQAdapter(colaEnsamblador),
    new BullMQAdapter(colaDistribuidor),
  ],
  serverAdapter: serverAdapter,
});

module.exports = {
  path: '/admin/queues',
  router: serverAdapter.getRouter()
};
