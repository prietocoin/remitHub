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
    const keyObjetivoR2 = key_r2 || (url_r2 ? url_r2.split('.dev/')[1] : null) || `comprobantes/${hash_largo}.jpg`;

    // 1. Obtener imagen desde Cloudflare R2
    const command = new GetObjectCommand({
      Bucket: process.env.R2_BUCKET_NAME || 'remesas-img',
      Key: keyObjetivoR2,
    });

    const s3Response = await s3Client.send(command);
    const byteArray = await s3Response.Body.transformToByteArray();
    const imageBase64 = Buffer.from(byteArray).toString('base64');
    const mimeType = s3Response.ContentType || 'image/jpeg';

    // 2. Ejecutar inferencia con la IA
    const prompt = `Eres un sistema quirúrgico experto en auditoría y extracción de datos financieros. Tu salida debe ser ÚNICAMENTE un objeto JSON válido, sin bloques de código (\`\`\`json) ni texto adicional.
${caption ? `Caption adjunto al mensaje: "${caption}"` : ''}

Extrae los campos de este comprobante de pago o transferencia con este formato exacto:
{
  "monto": number o null,
  "moneda": string o null (ej. "USD", "VES", "PEN", "EUR", "CLP"),
  "banco": string o null,
  "referencia": string o null,
  "titular": string o null
}`;

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
