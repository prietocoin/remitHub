const { Worker, Queue } = require('bullmq');
const Redis = require('ioredis');
const axios = require('axios');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');

// 1. Conexiones
const redisConnection = new Redis({
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: Number(process.env.REDIS_PORT) || 6379,
  password: process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: null,
});

const s3Client = new S3Client({
  region: 'auto',
  endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY_ID,
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
  },
});

// Cola de salida hacia el Ensamblador
const colaEnsamblador = new Queue('cola-ensamblador', { connection: redisConnection });

// 2. Gestión de llaves Gemini: Pool dinámico y rotación activa
const rawKeys = process.env.GEMINI_KEYS || process.env.GEMINI_API_KEY || '';
const geminiKeyList = rawKeys.split(',').map(k => k.trim().replace(/^["']|["']$/g, '')).filter(Boolean);
let currentKeyIndex = 0;

function getNextActiveKey() {
  if (geminiKeyList.length === 0) return null;
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
    console.warn('[Extractor Warning] No se pudo parsear el JSON de Gemini, devolviendo objeto vacío.');
    return {};
  }
}

// Función auxiliar para llamar a Gemini probando las llaves disponibles
async function llamarGeminiConFallback(prompt, mimeType, imageBase64) {
  if (geminiKeyList.length === 0) {
    throw new Error('No hay llaves de API de Gemini configuradas en GEMINI_KEYS o GEMINI_API_KEY');
  }

  let ultimoError = null;
  const maxIntentos = Math.min(geminiKeyList.length, 3); // Probar hasta 3 llaves distintas

  for (let intento = 0; intento < maxIntentos; intento++) {
    const activeKey = getNextActiveKey();
    const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${activeKey}`;

    try {
      const response = await axios.post(url, {
        contents: [{ parts: [{ text: prompt }, { inline_data: { mime_type: mimeType, data: imageBase64 } }] }],
        generationConfig: { response_mime_type: "application/json" }
      }, { timeout: 35000 });

      return response.data;
    } catch (err) {
      ultimoError = err;
      const status = err.response?.status;
      console.warn(`[Extractor LLM] Falló la llave en intento ${intento + 1}/${maxIntentos} (Status HTTP ${status || 'Unknown'}). Reintentando con siguiente llave...`);
    }
  }

  throw new Error(`Exhaustos todos los reintentos de llaves Gemini: ${ultimoError?.response?.data?.error?.message || ultimoError?.message}`);
}

// 3. Worker: Motor de Inferencia IA
const worker = new Worker('cola-extractor', async (job) => {
  const { hash_largo, instancia, key_r2 } = job.data;
  console.log(`[Extractor] 🧠 Iniciando análisis de comprobante con IA para Hash: ${hash_largo}`);

  try {
    // A. Descargar imagen desde Cloudflare R2
    console.log(`[Extractor] ☁️ Obteniendo objeto R2: ${key_r2}`);
    const command = new GetObjectCommand({
      Bucket: process.env.R2_BUCKET_NAME || 'remesas-img',
      Key: key_r2,
    });
    const s3Response = await s3Client.send(command);
    const byteArray = await s3Response.Body.transformToByteArray();
    const imageBase64 = Buffer.from(byteArray).toString('base64');
    const mimeType = s3Response.ContentType || 'image/jpeg';

    // B. Preparar prompt
    const prompt = `Analiza este comprobante de pago o transferencia y extrae estrictamente un objeto JSON:
    {
      "monto": number o null,
      "moneda": string o null (ej. "USD", "VES", "PEN", "EUR", "CLP"),
      "banco": string o null,
      "referencia": string o null,
      "titular": string o null
    }`;

    // C. Consultar API de Gemini con rotación y fallback
    const aiResponseData = await llamarGeminiConFallback(prompt, mimeType, imageBase64);
    const textResult = aiResponseData?.candidates?.[0]?.content?.parts?.[0]?.text;
    const datos_ia = parsearJSONSeguro(textResult);

    console.log(`[Extractor] ✨ Inferencia completada para Hash ${hash_largo}:`, JSON.stringify(datos_ia));

    // D. Encolar resultado para el Ensamblador
    await colaEnsamblador.add('ensamblar-datos', {
      hash_largo,
      instancia: instancia || 'JAIRO',
      datos_ia
    }, { removeOnComplete: true });

    console.log(`[Extractor] 🚀 Resultado encolado con éxito en "cola-ensamblador". Hash: ${hash_largo}`);

  } catch (error) {
    console.error(`[Extractor ERROR] Falló el procesamiento del Hash ${hash_largo}:`, error.message);
    throw error;
  }
}, { connection: redisConnection, concurrency: 3 });

// Escuchadores de eventos para la consola
worker.on('completed', (job) => {
  console.log(`[Extractor Evento] 🎉 Trabajo ${job.id} procesado con éxito.`);
});

worker.on('failed', (job, err) => {
  console.error(`[Extractor Evento] ❌ Trabajo ${job?.id} falló:`, err.message);
});

worker.on('error', (err) => {
  console.error('[Extractor Error de Red/Redis]', err.message);
});

console.log('[Extractor] 🟢 Worker iniciado y listo para procesar "cola-extractor".');
