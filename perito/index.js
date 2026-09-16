const { Worker } = require('bullmq');
const Redis = require('ioredis');
const { Pool } = require('pg');
const axios = require('axios');

// 1. Captura de Variables y Clave de Gemini (.env)
const rawKeys = process.env.GEMINI_KEYS || process.env.GEMINI_API_KEY || process.env.GEMINI_KEY || '';
const geminiKeyList = rawKeys.split(',').map(k => k.trim().replace(/^["']|["']$/g, '')).filter(Boolean);
const GEMINI_API_KEY = geminiKeyList[0] || '';

console.log(`[Perito Init] Gemini Key cargada: ${GEMINI_API_KEY ? 'SI (' + GEMINI_API_KEY.substring(0, 8) + '...)' : 'NO (Vacía)'}`);

// 2. Configuración de Conexiones
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

// 3. Función de Extracción con Gemini IA
async function extraerDatosComprobante(imageBase64, mimeType) {
  const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${GEMINI_API_KEY}`;

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

// 4. Worker Perito (Consumidor de la cola IA)
const worker = new Worker('cola-analisis-ia', async (job) => {
  const { hash_imagen, imageBase64, mimeType, instancia } = job.data;

  console.log(`[Perito Job] Analizando comprobante para hash_imagen: ${hash_imagen} (Instancia: ${instancia})`);

  try {
    // A. Extracción con Gemini IA
    const datos = await extraerDatosComprobante(imageBase64, mimeType);

    // B. Insertar / Actualizar en comprobantes_raw usando hash_imagen como clave
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

    // C. Cierre de Estado: Marcar registros_raw como 'PROCESADO'
    await pool.query(
      `UPDATE registros_raw SET estado = 'PROCESADO' WHERE hash_imagen = $1`,
      [hash_imagen]
    );

    console.log(`[Perito OK] Extracción exitosa e insertada en comprobantes_raw: ${hash_imagen}`);

  } catch (err) {
    console.error(`[Perito Error] Falló el análisis para ${hash_imagen}:`, err.message);

    // D. Marcar como FALLO para evitar bloqueos continuos en 'EN_COLA'
    await pool.query(
      `UPDATE registros_raw SET estado = 'FALLO' WHERE hash_imagen = $1`,
      [hash_imagen]
    );
    throw err;
  }
}, {
  connection,
  concurrency: 2
});

worker.on('failed', (job, err) => {
  console.error(`[Perito Job Failed] Tarea ${job?.data?.hash_imagen} falló:`, err.message);
});

worker.on('error', (err) => {
  console.error('[Perito Fatal Error]', err.message);
});

console.log('[perito-worker] Escuchando la cola cola-analisis-ia en Redis...');
