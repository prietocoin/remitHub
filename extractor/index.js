const { Worker } = require('bullmq');
const { GetObjectCommand } = require('@aws-sdk/client-s3');
const redisConfig = require('./src/config/redis');
const s3Client = require('./src/config/r2');
const colaEnsamblador = require('./src/queues/ensamblador.queue');
const { extraerDatosConGemini } = require('./src/services/gemini');

const worker = new Worker('cola-extractor', async (job) => {
  const { hash_largo, instancia, key_r2, url_r2, caption } = job.data;
  console.log(`[Extractor] 🧠 Analizando con Gemini para Hash: ${hash_largo?.slice(-8) || hash_largo}`);

  try {
    // Extracción segura de la clave en R2 (funciona con cualquier dominio o clave directa)
    const keyObjetivoR2 = key_r2 || (url_r2 ? decodeURIComponent(url_r2.replace(/^https?:\/\/[^\/]+\//, '')) : null) || `comprobantes/${hash_largo}.jpg`;

    // 1. Obtener imagen desde Cloudflare R2
    const command = new GetObjectCommand({
      Bucket: process.env.R2_BUCKET_NAME || 'remesas-img',
      Key: keyObjetivoR2,
    });

    const s3Response = await s3Client.send(command);
    const byteArray = await s3Response.Body.transformToByteArray();
    const imageBase64 = Buffer.from(byteArray).toString('base64');
    const mimeType = s3Response.ContentType || 'image/jpeg';

    // 2. Inferencia con la IA usando SYSTEM_PROMPT del .env
    const basePrompt = process.env.SYSTEM_PROMPT || 'Extrae los datos del comprobante en un JSON válido.';
    const prompt = caption ? `${basePrompt}\n\nCaption adjunto al mensaje: "${caption}"` : basePrompt;

    const datos_ia = await extraerDatosConGemini(prompt, mimeType, imageBase64);

    console.log(`[Extractor] ✨ Inferencia completada para Hash ${hash_largo?.slice(-8)}:`, JSON.stringify(datos_ia));

    // 3. Encolar al Ensamblador
    await colaEnsamblador.add('ensamblar-datos', {
      hash_largo,
      instancia: instancia || 'JAIRO',
      datos_ia
    });

    console.log(`[Extractor] 🚀 Derivado a "cola-ensamblador". Hash: ${hash_largo?.slice(-8)}`);

  } catch (error) {
    console.error(`[Extractor ERROR] Falló el Hash ${hash_largo}:`, error.message);
    throw error;
  }
}, { connection: redisConfig, concurrency: 3 });

worker.on('completed', (job) => console.log(`[Extractor Evento] 🎉 Job ${job.id} procesado.`));
worker.on('failed', (job, err) => console.error(`[Extractor Evento] ❌ Job ${job?.id} falló:`, err.message));

console.log('[Extractor] 🟢 Worker activo y listo.');
