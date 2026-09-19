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

const colaEnsamblador = new Queue('cola-ensamblador', { connection: redisConnection });

// 2. Gestión de llaves Gemini
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

// Función con soporte prioritario para gemini-3.5-flash-lite
async function llamarGeminiConFallback(prompt, mimeType, imageBase64) {
  if (geminiKeyList.length === 0) {
    throw new Error('No hay llaves de API de Gemini configuradas en GEMINI_KEYS o GEMINI_API_KEY');
  }

  // Prioridad 1: GEMINI_MODEL de variables o gemini-3.5-flash-lite exacto de tu n8n
  const modelosCandidatos = [
    process.env.GEMINI_MODEL,
    'gemini-3.5-flash-lite',
    'gemini-3.5-flash',
    'gemini-1.5-flash',
    'gemini-2.0-flash'
  ].filter(Boolean);

  let ultimoError = null;
  const maxIntentosKeys = Math.min(geminiKeyList.length, 3);

  for (let intentoKey = 0; intentoKey < maxIntentosKeys; intentoKey++) {
    const activeKey = getNextActiveKey();

    for (const rawModel of modelosCandidatos) {
      // Limpia el prefijo "models/" si viene definido en las variables de entorno
      const cleanModel = rawModel.replace(/^models\//, '');
      const url = `https://generativelanguage.googleapis.com/v1beta/models/${cleanModel}:generateContent?key=${activeKey}`;

      try {
        const response = await axios.post(url, {
          contents: [{
            parts: [
              { text: prompt },
              { inline_data: { mime_type: mimeType, data: imageBase64 } }
            ]
          }],
          generationConfig: { responseMimeType: "application/json" }
        }, { timeout: 35000 });

        return response.data;
      } catch (err) {
        ultimoError = err;
        const status = err.response?.status;
        const apiErrorMsg = err.response?.data?.error?.message || err.message;

        console.warn(`[Extractor LLM ⚠️] Modelo "${cleanModel}" falló (HTTP ${status \vert{}\vert{} 'Err'}):${apiErrorMsg}`);

        if (status === 404) {
          continue;
        }
        break;
      }
    }
  }

  throw new Error(`Exhaustos todos los reintentos de Gemini: ${ultimoError?.response?.data?.error?.message || ultimoError?.message}`);
}

// 3. Worker: Motor de Inferencia IA
const worker = new Worker('cola-extractor', async (job) => {
  const { hash_largo, instancia, key_r2, caption } = job.data;
  console.log(`[Extractor] 🧠 Iniciando análisis de comprobante con Gemini 3.5 Flash Lite para Hash: ${hash_largo}`);

  try {
    if (!key_r2) {
      throw new Error(`key_r2 vino nula/indefinida para el Hash ${hash_largo}. Imposible consultar Cloudflare R2.`);
    }

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

    // B. Prompt quirúrgico idéntico al nodo de n8n
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

    // C. Consultar API de Gemini
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
