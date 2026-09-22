const { Queue } = require('bullmq');
const { createBullBoard } = require('@bull-board/api');
const { BullMQAdapter } = require('@bull-board/api/bullMQAdapter');
const { ExpressAdapter } = require('@bull-board/express');
const redisConfig = require('../config/redis');

// Configuración del adaptador
const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath('/admin/queues');

// Instancia de las 4 colas
const colaValidador = new Queue('cola-validador', { connection: redisConfig });
const colaEnsamblador = new Queue('cola-ensamblador', { connection: redisConfig });
const colaDistribuidor = new Queue('cola-distribuidor', { connection: redisConfig });
const colaExtractor = new Queue('cola-extractor', { connection: redisConfig }); // <-- Agregada

// Creación del tablero
createBullBoard({
  queues: [
    new BullMQAdapter(colaValidador),
    new BullMQAdapter(colaEnsamblador),
    new BullMQAdapter(colaDistribuidor),
    new BullMQAdapter(colaExtractor), // <-- Agregada
  ],
  serverAdapter: serverAdapter,
});

// Exportar directamente el router de Express
module.exports = serverAdapter.getRouter();
