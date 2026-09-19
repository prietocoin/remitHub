const express = require('express');
const { Queue } = require('bullmq');
const Redis = require('ioredis');

const app = express();
app.use(express.json({ limit: '10mb' }));

// Conexión Redis
const connection = new Redis({
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: Number(process.env.REDIS_PORT) || 6379,
  password: process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: null,
});

const colaValidador = new Queue('cola-validador', { connection });

app.get('/', (req, res) => res.status(200).json({ status: 'ok', service: 'ingesta-api' }));

app.post('/api/v1/webhook/whatsapp', async (req, res) => {
  // 1. IMPRIMIR INMEDIATAMENTE EN CONSOLA APENAS LLEGA LA PETICIÓN
  console.log('====================================');
  console.log('[Ingesta] 📥 PETICIÓN ENTRANTE DESDE N8N');
  console.log('[Ingesta] Payload recibido:', JSON.stringify(req.body, null, 2));
  console.log('====================================');

  // Responder a n8n para no trabar el flujo
  res.status(200).json({ status: 'processing' });

  try {
    const body = Array.isArray(req.body) ? req.body[0] : req.body;
    const data = body?.data || body || {};
    const key = data?.key || {};
    const message = data?.message || {};

    // Extraer hash probando todas las estructuras posibles
    const hash_largo = key.id || body.hash_largo || data.hash_largo;

    if (!hash_largo) {
      console.log('[Ingesta] ⚠️ ATENCIÓN: Se recibió el webhook pero NO SE ENCONTRÓ ningún "hash_largo" ni "key.id" en el JSON.');
      return;
    }

    console.log(`[Ingesta] ✅ Hash detectado correctamente: ${hash_largo}. Encolando en Redis...`);

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

    await colaValidador.add('validar-evento', payload, {
      removeOnComplete: true,
      attempts: 3,
      backoff: { type: 'exponential', delay: 2000 }
    });

    console.log(`[Ingesta] 🚀 Evento empujado a Redis exitosamente. Queue: cola-validador | Hash: ${hash_largo}`);

  } catch (error) {
    console.error('[Ingesta ERROR]', error.message);
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`[ingesta-api] Escuchando en puerto ${PORT}`);
});
