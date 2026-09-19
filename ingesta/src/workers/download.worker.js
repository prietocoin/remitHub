const { Worker } = require('bullmq');
const { PutObjectCommand } = require('@aws-sdk/client-s3');
const redisConfig = require('../config/redis');
const s3Client = require('../config/r2');
const { obtenerBufferImagen } = require('../services/evolution');
const { generarSha256 } = require('../services/hash');
const validadorQueue = require('../queues/validador.queue');

function extraerSha256Nativo(fileShaObj) {
  if (!fileShaObj) return null;
  try {
    if (Buffer.isBuffer(fileShaObj)) return fileShaObj.toString('hex').toUpperCase();
    if (typeof fileShaObj === 'string') return fileShaObj.toUpperCase();
    if (typeof fileShaObj === 'object') {
      const bytes = Object.values(fileShaObj);
      if (bytes.length === 32) return Buffer.from(bytes).toString('hex').toUpperCase();
    }
  } catch (e) {
    return null;
  }
  return null;
}

const downloadWorker = new Worker('cola-descarga-media', async (job) => {
  const { rawPayload } = job.data;
  
  const item = Array.isArray(rawPayload) ? rawPayload[0] : rawPayload;
  const body = item?.body || item || {};
  const data = body?.data || {};
  
  const key = data?.key || {};
  const message = data?.message || {};
  const imageMsg = message?.imageMessage;

  // Extracción 100% Dinámica Multitenant
  const instancia = body?.instance || data?.instance || null;
  const instanceId = data?.instanceId || body?.instanceId || null;

  const es_imagen = Boolean(imageMsg || body?.es_imagen);
  let imageBuffer = null;

  // 1. Descargar imagen desde Evolution API usando resolutor multitenant
  if (es_imagen && key.id) {
    console.log(`[Worker Descarga] 🔄 Pidiendo imagen a Evolution API (Tenant: "${instancia || instanceId}")...`);
    imageBuffer = await obtenerBufferImagen(instancia, instanceId, key);
  }

  // 2. Resolver SHA-256 (Prioridad: WhatsApp Nativo > Buffer > Fallback)
  const shaNativoWhatsApp = extraerSha256Nativo(imageMsg?.fileSha256);
  let hash_largo = null;

  if (shaNativoWhatsApp) {
    hash_largo = shaNativoWhatsApp;
    console.log(`[Worker Descarga] 🎯 SHA-256 Nativo: ${hash_largo}`);
  } else if (imageBuffer) {
    hash_largo = generarSha256(imageBuffer);
    console.log(`[Worker Descarga] 🟢 SHA-256 desde Buffer: ${hash_largo}`);
  } else {
    const keyId = key.id || `msg_${Date.now()}`;
    hash_largo = generarSha256(null, keyId);
    console.log(`[Worker Descarga] ⚠️ SHA-256 Fallback (key.id): ${hash_largo}`);
  }

  const hash_corto = hash_largo.slice(-8);

  // 3. Subir a Cloudflare R2 si se obtuvo el Buffer
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
      console.log(`[Worker Descarga] ☁️ Guardado en R2: ${key_r2}`);
    } catch (r2Err) {
      console.error('[Worker Descarga ERROR] Fallo subiendo a R2:', r2Err.message);
    }
  }

  // 4. Encaminar al Validador
  const payloadValidador = {
    hash_largo,
    hash_corto,
    key_r2,
    url_r2,
    grupo_raw: key.remoteJid || body.grupo_raw || '',
    usuario_raw: key.participantAlt || key.participant || key.remoteJid || body.usuario_raw || '',
    nombre_push: data.pushName || body.nombre_push || 'Desconocido',
    caption: imageMsg?.caption || message.conversation || body.caption || '',
    timestamp_msg: Number(data.messageTimestamp || body.timestamp_msg || Math.floor(Date.now() / 1000)),
    es_imagen: Boolean(imageBuffer || es_imagen),
    instancia: instancia || instanceId || 'desconocida',
    payload_raw: rawPayload
  };

  await validadorQueue.add('validar-evento', payloadValidador);
  console.log(`[Worker Descarga] 🚀 Enviado a "cola-validador". SHA-256: ${hash_largo}`);

}, {
  connection: redisConfig,
  concurrency: 5
});

downloadWorker.on('failed', (job, err) => {
  console.error(`[Worker Descarga ERROR] Job ${job?.id} falló:`, err.message);
});

module.exports = downloadWorker;
