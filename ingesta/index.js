const express = require('express');
const { Queue } = require('bullmq');
const Redis = require('ioredis');

const app = express();
app.use(express.json({ limit: '10mb' }));

// 1. Conexión a Redis con tipado correcto y manejo de variables
const connection = new Redis({
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: Number(process.env.REDIS_PORT) || 6379,
  password: process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: null,
});

// 2. Apuntar a la cola de destino
const colaValidador = new Queue('cola-validador', { connection });

app.get('/', (req, res) => res.status(200).json({ status: 'ok', service: 'ingesta-api' }));

app.post('/api/v1/webhook/whatsapp', async (req, res) => {
  // Liberar n8n inmediatamente
  res.status(200).json({ status: 'processing' });

  try {
    const body = Array.isArray(req.body) ? req.body[0] : req.body;
    const data = body.data || body;
    const key = data.key || {};
    const message = data.message || {};

    // Extraer hash con soporte extendido
    const hash_largo = key.id || body.hash_largo || data.hash_largo;

    console.log(`[Ingesta] 📥 Webhook recibido. Hash detectado: ${hash_largo || 'NINGUNO'}`);

    if (!hash_largo) {
      console.warn('[Ingesta] ⚠️ Petición descartada: El payload de n8n no contiene "key.id" ni "hash_largo".');
      return;
    }

    // Empaquetado robusto con soporte para "instancia"
    const payload = {
      hash_largo,
      hash_corto: body.hash_corto || (hash_largo.length >= 8 ? hash_largo.slice(-8) : hash_largo),
      grupo_raw: key.remoteJid || body.grupo_raw || '',
      usuario_raw: key.participantAlt || key.participant || key.remoteJid || body.usuario_raw || '',
      nombre_push: data.pushName || body.nombre_push || 'Desconocido',
      caption: message.imageMessage?.caption || message.conversation || message.extendedTextMessage?.text || body.caption || '',
      timestamp_msg: Number(data.messageTimestamp || body.timestamp_msg || Math.floor(Date.now() / 1000)),
      es_imagen: Boolean(body.es_imagen || body.imagen_base64 || message.imageMessage),
      instancia: body.instancia || body.instance || data.instancia || data.instance || 'JAIRO',
      url_imagen: body.url_imagen || data.url_imagen || null
    };

    // Empujar a BullMQ
    await colaValidador.add('validar-evento', payload, {
      removeOnComplete: true,
      attempts: 3,
      backoff: { type: 'exponential', delay: 2000 }
    });

    console.log(`[Ingesta] 🚀 Evento encolado con éxito en "cola-validador". Hash: ${hash_largo}`);

  } catch (error) {
    console.error('[Ingesta Error]', error.message);
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`[ingesta-api] Escuchando en puerto ${PORT} - Modo Amortiguador Activo`);
});
