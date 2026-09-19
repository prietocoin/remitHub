const express = require('express');
const { Queue } = require('bullmq');
const Redis = require('ioredis');
const { Pool } = require('pg');

const app = express();
app.use(express.json({ limit: '10mb' }));

// 1. Conexiones a BD y Redis
const pool = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT) || 5432,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

const connection = new Redis({
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: process.env.REDIS_PORT || 6379,
  password: process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: null,
});

// 2. Apuntar a la nueva cola del siguiente módulo
const colaValidador = new Queue('cola-validador', { connection });

app.get('/', (req, res) => res.status(200).json({ status: 'ok', service: 'ingesta-api' }));

app.post('/api/v1/webhook/whatsapp', async (req, res) => {
  // Responde 200 OK inmediatamente a la fuente para evitar timeouts
  res.status(200).json({ status: 'processing' });

  try {
    const body = Array.isArray(req.body) ? req.body[0] : req.body;
    const data = body.data || body;
    const key = data.key || {};
    const message = data.message || {};

    const hash_largo = key.id || body.hash_largo;
    if (!hash_largo) return;

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
      url_imagen: body.url_imagen || data.url_imagen || null // Captura de URL si viene en el webhook
    };

    // 3. Backup Inmutable y lógica de conteo (2x) nativa en SQL
    await pool.query(`
      INSERT INTO registros_raw (
        hash_largo, hash_corto, grupo_raw, usuario_raw, nombre_push, 
        caption, timestamp_msg, es_imagen, instancia, url_imagen, conteo, estado
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1, 'RECIBIDO')
      ON CONFLICT (hash_largo) DO UPDATE SET 
        conteo = registros_raw.conteo + 1,
        caption = COALESCE(NULLIF(EXCLUDED.caption, ''), registros_raw.caption),
        es_imagen = EXCLUDED.es_imagen OR registros_raw.es_imagen,
        url_imagen = COALESCE(EXCLUDED.url_imagen, registros_raw.url_imagen);
    `, [
      payload.hash_largo, payload.hash_corto, payload.grupo_raw, payload.usuario_raw,
      payload.nombre_push, payload.caption, payload.timestamp_msg, payload.es_imagen,
      payload.instancia, payload.url_imagen
    ]);

    // 4. Encolar evento ligero para el Validador
    await colaValidador.add('validar-evento', { 
      hash_largo: payload.hash_largo, 
      instancia: payload.instancia 
    }, {
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
  console.log(`[ingesta-api] Escuchando en puerto ${PORT}`);
});
