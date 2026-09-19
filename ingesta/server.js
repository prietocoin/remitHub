const express = require('express');
const downloadQueue = require('./src/queues/download.queue');

const app = express();
app.use(express.json({ limit: '50mb' }));

app.get('/', (req, res) => res.status(200).json({ status: 'ok', service: 'ingesta-api' }));

app.post('/api/v1/webhook/whatsapp', async (req, res) => {
  // Responder a WhatsApp/n8n de inmediato
  res.status(200).json({ status: 'queued', timestamp: Date.now() });

  try {
    // Persistir el payload crudo en la cola de Redis
    await downloadQueue.add('procesar-media', { rawPayload: req.body });
    console.log('[Express Ingesta] 📥 Webhook recibido y encolado en "cola-descarga-media"');
  } catch (err) {
    console.error('[Express Ingesta ERROR] Fallo al encolar en Redis:', err.message);
  }
});

module.exports = app;
