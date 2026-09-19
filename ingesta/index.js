const express = require('express');
const { Queue } = require('bullmq');
const Redis = require('ioredis');

const app = express();
app.use(express.json({ limit: '10mb' }));

// 1. Conexión exclusiva a Redis (El amortiguador)
const connection = new Redis({
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: process.env.REDIS_PORT || 6379,
  password: process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: null,
});

// 2. Apuntar a la cola donde el Validador recogerá el trabajo
const colaValidador = new Queue('cola-validador', { connection });

app.get('/', (req, res) => res.status(200).json({ status: 'ok', service: 'ingesta-api' }));

app.post('/api/v1/webhook/whatsapp', async (req, res) => {
  // 3. Liberar la conexión de WhatsApp/n8n en el milisegundo cero
  res.status(200).json({ status: 'processing' });

  try {
    const body = Array.isArray(req.body) ? req.body[0] : req.body;
    const data = body.data || body;
    const key = data.key || {};
    const message = data.message || {};

    const hash_largo = key.id || body.hash_largo;
    if (!hash_largo) return;

    // 4. Empaquetar todo lo que llegó
    const payload = {
      hash_largo,
      hash_corto: body.hash_corto || (hash_largo.length >= 8 ? hash_largo.slice(-8) : hash_largo),
      grupo_raw: key.remoteJid || body.grupo_raw || '',
      usuario_raw: key.participantAlt || key.participant || key.remoteJid || body.usuario_raw || '',
      nombre_push: data.pushName || body.nombre_push || 'Desconocido',
      caption: message.imageMessage?.caption || message.conversation || message.extendedTextMessage?.text || body.caption || '',
      timestamp_msg: Number(data.messageTimestamp || body.timestamp_msg || Math.floor(Date.now() / 1000)),
      es_imagen: Boolean(body.es_imagen || body.imagen_base64 || message.imageMessage),
      instancia: body.instance || data.instance || 'default',
      url_imagen: body.url_imagen || data.url_imagen || null
    };

    // 5. Arrojar a BullMQ y olvidar
    await colaValidador.add('validar-evento', payload, {
      removeOnComplete: true,
      attempts: 3,
      backoff: { type: 'exponential', delay: 2000 }
    });

  } catch (error) {
    console.error('[Ingesta Error]', error.message);
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`[ingesta-api] Escuchando en puerto ${PORT} - Modo Amortiguador Activo`);
});
