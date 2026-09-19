const { Worker, Queue } = require('bullmq');
const Redis = require('ioredis');
const axios = require('axios');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');

// 1. Conexión exclusiva a Redis
const redisConnection = new Redis({
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: process.env.REDIS_PORT || 6379,
});

const s3Client = new S3Client({
  region: 'auto',
  endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY_ID,
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
  },
});

const colaEnsamblador = new Queue('cola-ensamblador', { connection: redisConnection });

// 2. Gestión de llaves: Rotación Preventiva (Round-Robin)
const rawKeys = process.env.GEMINI_KEYS || process.env.GEMINI_API_KEY || '';
const geminiKeyList = rawKeys.split(',').map(k => k.trim().replace(/^["']|["']$/g, '')).filter(Boolean);
let currentKeyIndex = 0;

// NUEVA LÓGICA: Obtiene la llave actual y rota el índice inmediatamente para el siguiente uso
function getNextActiveKey() {
  if (geminiKeyList.length === 0) return '';
  const key = geminiKeyList[currentKeyIndex % geminiKeyList.length];
  currentKeyIndex = (currentKeyIndex + 1) % geminiKeyList.length; 
  return key;
}

function parsearJSONSeguro(texto) {
  if (!texto) return {};
  const limpio = texto.replace(/^```json/gi, '').replace(/```$/g, '').trim();
  try {
    return JSON.parse(limpio);
  } catch (e) {
    return {};
  }
}

// 3. Worker: Motor de Inferencia IA
const worker = new Worker('cola-extractor', async (job) => {
  const { hash_largo, instancia, key_r2 } = job.data;
  console.log(`[Extractor] Iniciando análisis IA para: ${hash_largo}`);

  try {
    const command = new GetObjectCommand({
      Bucket: process.env.R2_BUCKET_NAME || 'remesas-img',
      Key: key_r2,
    });
    const s3Response = await s3Client.send(command);
    const byteArray = await s3Response.Body.transformToByteArray();
    const imageBase64 = Buffer.from(byteArray).toString('base64');
    const mimeType = s3Response.ContentType || 'image/jpeg';

    // Se asigna la llave y ya queda rotada para la próxima imagen
    const activeKey = getNextActiveKey();
    const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${activeKey}`;
    
    const prompt = `Analiza este comprobante de pago o transferencia y extrae estrictamente un objeto JSON:
    {
      "monto": number o null,
      "moneda": string o null (ej. "USD", "VES", "PEN", "EUR", "CLP"),
      "banco": string o null,
      "referencia": string o null,
      "titular": string o null
    }`;

    const aiResponse = await axios.post(url, {
      contents: [{ parts: [{ text: prompt }, { inline_data: { mime_type: mimeType, data: imageBase64 } }] }],
      generationConfig: { response_mime_type: "application/json" }
    }, { timeout: 35000 });

    const textResult = aiResponse.data?.candidates?.[0]?.content?.parts?.[0]?.text;
    const datos_ia = parsearJSONSeguro(textResult);

    await colaEnsamblador.add('ensamblar-datos', {
      hash_largo,
      instancia,
      datos_ia
    }, { removeOnComplete: true });

    console.log(`[Extractor] Inferencia exitosa. JSON encolado para: ${hash_largo}`);

  } catch (error) {
    console.error(`[Extractor Error] Falló ${hash_largo}:`, error.response?.data?.error?.message || error.message);
    throw error; 
  }
}, { connection: redisConnection, concurrency: 3 });

console.log('[Extractor] Worker iniciado con rotación preventiva de llaves...');
