const { GetObjectCommand } = require('@aws-sdk/client-s3');
const s3Client = require('../../../config/r2');
const { extraerDatosConGemini } = require('./gemini');

async function procesarExtraccionIA(data) {
  const { hash_largo, key_r2, url_r2, caption } = data;
  console.log(`[Extractor] 🧠 Analizando con Gemini para Hash: ${hash_largo?.slice(-8) || hash_largo}`);

  // 1. Resolver clave de Cloudflare R2
  const keyObjetivoR2 = key_r2 || (url_r2 ? decodeURIComponent(url_r2.replace(/^https?:\/\/[^\/]+\//, '')) : null) || `comprobantes/${hash_largo}.jpg`;

  // 2. Obtener imagen desde Cloudflare R2
  const command = new GetObjectCommand({
    Bucket: process.env.R2_BUCKET_NAME || 'remesas-img',
    Key: keyObjetivoR2,
  });

  const s3Response = await s3Client.send(command);
  const byteArray = await s3Response.Body.transformToByteArray();
  const imageBase64 = Buffer.from(byteArray).toString('base64');
  const mimeType = s3Response.ContentType || 'image/jpeg';

  // 3. Inferencia con la IA usando SYSTEM_PROMPT
  const basePrompt = process.env.SYSTEM_PROMPT || 'Extrae los datos del comprobante en un JSON válido.';
  const prompt = caption ? `${basePrompt}\n\nCaption adjunto al mensaje: "${caption}"` : basePrompt;

  const datosIA = await extraerDatosConGemini(prompt, mimeType, imageBase64);

  console.log(`[Extractor] ✨ Inferencia completada para Hash ${hash_largo?.slice(-8)}:`, JSON.stringify(datosIA));

  return datosIA;
}

module.exports = { procesarExtraccionIA };
