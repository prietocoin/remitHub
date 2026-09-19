const { Worker } = require('bullmq');
const { PutObjectCommand } = require('@aws-sdk/client-s3');
const redisConfig = require('../config/redis');
const s3Client = require('../config/r2');
const { obtenerBufferImagen } = require('../services/evolution');
const { generarSha256 } = require('../services/hash');
const validadorQueue = require('../queues/validador.queue');

const downloadWorker = new Worker('cola-descarga-media', async (job) => {
  const { rawPayload } = job.data;
  const body = Array.isArray(rawPayload) ? rawPayload[0] : rawPayload;
  const data = body?.data || body || {};
  const key = data?.key || {};
  const message = data?.message || {};
  const instancia = body.instancia || body.instance || data.instance || 'JAIRO';

  const es_imagen = Boolean(message.imageMessage || body.es_imagen);
  let imageBuffer = null;

  // 1. Descargar imagen desde Evolution API si aplica
  if (es_imagen) {
    console.log(`[Worker Descarga] 🔄 Solicitando imagen a Evolution API (${instancia})...`);
    imageBuffer = await obtenerBufferImagen(instancia, key, message);
  }

  // --- LOG DE DIAGNÓSTICO ---
  if (imageBuffer) {
    console.log(`[Worker Descarga] 🟢 Buffer obtenido con éxito (${imageBuffer.length} bytes). Generando SHA-256 del BINARIO.`);
  } else {
    console.log(`[Worker Descarga] ⚠️ ALERTA: Buffer es NULL. Usando Fallback de key.id (${key.id}).`);
  }
  // ---------------------------

  // 2. Generar huella SHA-256 (64 caracteres Hex)
  const hash_largo = generarSha256(imageBuffer, key.id || body.key_id);
  const hash_corto = hash_largo.slice(-8);
 

  // 3. Subir a Cloudflare R2 si existe la imagen
  let key_r2 = null;
  let url_r2 = null;

  if (imageBuffer) {
    key_r2 = `comprobantes/${hash_largo}.jpg`;
    const bucketName = process.env.R2_BUCKET_NAME || 'remesas-img';

    await s3Client.send(new PutObjectCommand({
      Bucket: bucketName,
      Key: key_r2,
      Body: imageBuffer,
      ContentType: 'image/jpeg',
    }));

    const publicDomain = process.env.R2_PUBLIC_DOMAIN || '';
    url_r2 = publicDomain ? `${publicDomain}/${key_r2}` : key_r2;
    console.log(`[Worker Descarga] ☁️ Guardado en R2: ${key_r2}`);
  }

  // 4. Armar Payload unificado y mandar a cola-validador
  const payloadValidador = {
    hash_largo,
    hash_corto,
    key_r2,
    url_r2,
    grupo_raw: key.remoteJid || body.grupo_raw || '',
    usuario_raw: key.participantAlt || key.participant || key.remoteJid || body.usuario_raw || '',
    nombre_push: data.pushName || body.nombre_push || 'Desconocido',
    caption: message.imageMessage?.caption || message.conversation || body.caption || '',
    timestamp_msg: Number(data.messageTimestamp || body.timestamp_msg || Math.floor(Date.now() / 1000)),
    es_imagen: Boolean(imageBuffer || es_imagen),
    instancia,
    payload_raw: rawPayload
  };

  await validadorQueue.add('validar-evento', payloadValidador);
  console.log(`[Worker Descarga] 🚀 Evento enviado a "cola-validador". SHA-256: ${hash_largo}`);

}, {
  connection: redisConfig,
  concurrency: 3 // Control de concurrencia para proteger la RAM
});

downloadWorker.on('failed', (job, err) => {
  console.error(`[Worker Descarga ERROR] Job ${job?.id} falló:`, err.message);
});

module.exports = downloadWorker;
