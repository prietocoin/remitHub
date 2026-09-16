const { Worker } = require('bullmq');
const Redis = require('ioredis');
const { Pool } = require('pg');
const axios = require('axios');

// 1. Carga y Rotación de Claves (.env)
const rawKeys = process.env.GEMINI_KEYS || process.env.GEMINI_API_KEY || process.env.GEMINI_KEY || '';
const geminiKeyList = rawKeys.split(',').map(k => k.trim().replace(/^["']|["']$/g, '')).filter(Boolean);
let currentKeyIndex = 0;

function getActiveKey() {
  if (geminiKeyList.length === 0) return '';
  return geminiKeyList[currentKeyIndex % geminiKeyList.length];
}

function rotateKey() {
  if (geminiKeyList.length > 1) {
    currentKeyIndex = (currentKeyIndex + 1) % geminiKeyList.length;
    console.log(`[Perito] Rotando a API Key índice: ${currentKeyIndex}`);
  }
}

// 2. Conexiones
const pool = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT) || 5432,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

const connection = new Redis({
  host: process.env.REDIS_HOST,
  port: Number(process.env.REDIS_PORT) || 6379,
  password: process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: null,
});

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// 3. Extracción con Gemini IA
async function extraerDatosComprobante(imageBase64, mimeType) {
  const activeKey = getActiveKey();
  const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-3.5-flash:generateContent?key=${activeKey}`;

  const prompt = `Analiza este comprobante de pago o transferencia y extrae estrictamente un objeto JSON con los siguientes campos:
  {
    "monto": number o null,
    "moneda": "USD" | "VES" | "EUR" | null,
    "banco": string o null,
    "referencia": string o null,
    "titular": string o null
  }`;

  const response = await axios.post(
    url,
    {
      contents: [{
        parts: [
          { text: prompt },
          { inline_data: { mime_type: mimeType || 'image/jpeg', data: imageBase64 } }
        ]
      }],
      generationConfig: { response_mime_type: "application/json" }
    },
    { timeout: 30000 }
  );

  const textResult = response.data?.candidates?.[0]?.content?.parts?.[0]?.text;
  return JSON.parse(textResult || '{}');
}

// 4. Worker con Resiliencia ante Errores 503 / 429
const worker = new Worker('cola-analisis-ia', async (job) => {
  const { hash_imagen, imageBase64, mimeType, instancia } = job.data;

  // Pausa preventiva de 12s para cumplir cuota de API
  await sleep(12000);

  console.log(`[Perito Job] Analizando: ${hash_imagen} (Instancia: ${instancia})`);

  try {
    const datos = await extraerDatosComprobante(imageBase64, mimeType);

    await pool.query(`
      INSERT INTO comprobantes_raw (
        hash_largo, monto, moneda, banco, referencia, titular, procesado_ia
      )
      VALUES ($1, $2, $3, $4, $5, $6, true)
      ON CONFLICT (hash_largo) DO UPDATE SET
        monto = EXCLUDED.monto,
        moneda = EXCLUDED.moneda,
        banco = EXCLUDED.banco,
        referencia = EXCLUDED.referencia,
        titular = EXCLUDED.titular,
        procesado_ia = true;
    `, [
      hash_imagen,
      datos.monto || null,
      datos.moneda || null,
      datos.banco || null,
      datos.referencia || null,
      datos.titular || null
    ]);

    await pool.query(
      `UPDATE registros_raw SET estado = 'PROCESADO' WHERE hash_imagen = $1`,
      [hash_imagen]
    );

    console.log(`[Perito OK] Procesado exitoso: ${hash_imagen}`);

  } catch (err) {
    const status = err.response?.status;
    const errText = (err.response?.data?.error?.message || err.message || '').toLowerCase();
    
    // Identificar si es un error temporal (429 Cuota, 503/500 Servidor ocupado o Timeout)
    const isTransientError = status === 429 || 
                             status >= 500 || 
                             errText.includes('quota') || 
                             errText.includes('exceeded') || 
                             errText.includes('rate') ||
                             err.code === 'ECONNRESET' ||
                             err.code === 'ETIMEDOUT';

    if (isTransientError) {
      rotateKey();
      console.warn(`[Perito Falla Temporal HTTP ${status || err.code}] Reintentando ${hash_imagen} (vuelve a PENDIENTE).`);
      
      await pool.query(
        `UPDATE registros_raw SET estado = 'PENDIENTE' WHERE hash_imagen = $1`,
        [hash_imagen]
      );
      
      // Pausa de enfriamiento adicional tras error 503 / 429
      await sleep(15000);
    } else {
      console.error(`[Perito Error Fatal] ${hash_imagen}:`, err.message);
      await pool.query(
        `UPDATE registros_raw SET estado = 'FALLO' WHERE hash_imagen = $1`,
        [hash_imagen]
      );
    }
  }
}, {
  connection,
  concurrency: 1
});

worker.on('failed', (job, err) => console.error(`[Perito Job Failed] ID: ${job?.data?.hash_imagen}`));
worker.on('error', (err) => console.error('[Perito Fatal Error]', err.message));

console.log('[perito-worker] Escuchando cola-analisis-ia con control de fallos 503/429...');
