const express = require('express');
const crypto = require('crypto');
const { Queue } = require('bullmq');
const Redis = require('ioredis');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

const app = express();
// Subimos el límite a 50mb para recibir buffers base64 pesados sin rechazos
app.use(express.json({ limit: '50mb' }));

// 1. Conexión Redis
const connection = new Redis({
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: Number(process.env.REDIS_PORT) || 6379,
  password: process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: null,
});

const colaValidador = new Queue('cola-validador', { connection });

// 2. Cliente Cloudflare R2 (Construido con R2_ACCOUNT_ID)
const s3Client = new S3Client({
  region: 'auto',
  endpoint: process.env.R2_ENDPOINT || `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY_ID || '',
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY || '',
  },
});

app.get('/', (req, res) => res.status(200).json({ status: 'ok', service: 'ingesta-api' }));

app.post('/api/v1/webhook/whatsapp', async (req, res) => {
  console.log('====================================');
  console.log('[Ingesta] 📥 PETICIÓN ENTRANTE DESDE N8N');
  console.log('====================================');

  // Responder a n8n de inmediato para no trabar el webhook
  res.status(200).json({ status: 'processing' });

  try {
    const body = Array.isArray(req.body) ? req.body[0] : req.body;
    const data = body?.data || body || {};
    const key = data?.key || {};
    const message = data?.message || {};

    // 3. Extracción del Buffer de Imagen
    let rawBase64 = body.imagen_base64 || body.base64 || message.imageMessage?.base64 || null;
    let imageBuffer = null;

    if (rawBase64) {
      const cleanBase64 = rawBase64.replace(/^data:image\/\w+;base64,/, '');
      imageBuffer = Buffer.from(cleanBase64, 'base64');
    }

    // 4. Generación de Huella MD5 (Garantiza Binomio 2x)
    let hash_largo;
    const fileSha256 = message.imageMessage?.fileSha256;

    if (imageBuffer) {
      // Opción A: MD5 real generado sobre el binario de la imagen
      hash_largo = crypto.createHash('md5').update(imageBuffer).digest('hex').toUpperCase();
    } else if (fileSha256) {
      // Opción B: MD5 generado sobre el Sha256 nativo de la foto entregado por WhatsApp
      const shaStr = typeof fileSha256 === 'string' ? fileSha256 : JSON.stringify(fileSha256);
      hash_largo = crypto.createHash('md5').update(shaStr).digest('hex').toUpperCase();
    } else {
      // Fallback para mensajes de solo texto
      const keyId = key.id || body.hash_largo || body.key_id || `msg_${Date.now()}`;
      hash_largo = crypto.createHash('md5').update(String(keyId)).digest('hex').toUpperCase();
    }

    const hash_corto = hash_largo.slice(-8);
    console.log(`[Ingesta] 🔑 Huella MD5 calculada: ${hash_largo}`);

    // 5. Subida Inmediata a Cloudflare R2 (Previene expiración de URLs)
    let key_r2 = null;
    let url_r2 = null;

    if (imageBuffer) {
      key_r2 = `comprobantes/${hash_largo}.jpg`;
      const bucketName = process.env.R2_BUCKET_NAME || 'remesas-img';

      try {
        await s3Client.send(new PutObjectCommand({
          Bucket: bucketName,
          Key: key_r2,
          Body: imageBuffer,
          ContentType: 'image/jpeg',
        }));

        const publicDomain = process.env.R2_PUBLIC_DOMAIN || '';
        url_r2 = publicDomain ? `${publicDomain}/${key_r2}` : key_r2;
        console.log(`[Ingesta] ☁️ Imagen respaldada con éxito en R2: ${key_r2}`);
      } catch (r2Err) {
        console.error('[Ingesta ERROR] Fallo al subir imagen a R2:', r2Err.message);
      }
    }

    // 6. Ensamblar Payload Estandarizado para el Validador
    const payload = {
      hash_largo,
      hash_corto,
      key_r2,
      url_r2,
      grupo_raw: key.remoteJid || body.grupo_raw || '',
      usuario_raw: key.participantAlt || key.participant || key.remoteJid || body.usuario_raw || '',
      nombre_push: data.pushName || body.nombre_push || 'Desconocido',
      caption: message.imageMessage?.caption || message.conversation || message.extendedTextMessage?.text || body.caption || '',
      timestamp_msg: Number(data.messageTimestamp || body.timestamp_msg || Math.floor(Date.now() / 1000)),
      es_imagen: Boolean(imageBuffer || message.imageMessage || body.es_imagen),
      instancia: body.instancia || body.instance || data.instancia || data.instance || 'JAIRO',
      payload_raw: req.body
    };

    // 7. Empujar a la Cola de Redis
    await colaValidador.add('validar-evento', payload, {
      removeOnComplete: true,
      attempts: 3,
      backoff: { type: 'exponential', delay: 2000 }
    });

    console.log(`[Ingesta] 🚀 Evento encolado en "cola-validador". Hash: ${hash_largo} | R2: ${key_r2 || 'N/A'}`);

  } catch (error) {
    console.error('[Ingesta ERROR]', error.message);
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`[ingesta-api] Escuchando en puerto ${PORT}`);
});
