const { Pool } = require('pg');
const axios = require('axios');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');

// Configuración de la base de datos PostgreSQL
const pool = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT) || 5432,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

// Configuración del cliente S3 para Cloudflare R2
const s3Client = new S3Client({
  region: 'auto',
  endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY_ID || '',
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY || '',
  },
});

// Gestión de claves de la API de Gemini
const rawKeys = process.env.GEMINI_KEYS || process.env.GEMINI_API_KEY || process.env.GEMINI_KEY || '';
const geminiKeyList = rawKeys.split(',').map(k => k.trim().replace(/^["']|["']$/g, '')).filter(Boolean);
let currentKeyIndex = 0;

function getActiveKey() {
  return geminiKeyList[currentKeyIndex % geminiKeyList.length] || '';
}

function rotateKey() {
  if (geminiKeyList.length > 1) {
    currentKeyIndex = (currentKeyIndex + 1) % geminiKeyList.length;
  }
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function parsearJSONSeguro(texto) {
  if (!texto) return {};
  const limpio = texto.replace(/```json/gi, '').replace(/```/g, '').trim();
  try {
    return JSON.parse(limpio);
  } catch (e) {
    return {};
  }
}

// Extrae la clave relativa de la imagen a partir de la URL almacenada en BD
function obtenerClaveR2(url) {
  try {
    const parsedUrl = new URL(url);
    return decodeURIComponent(parsedUrl.pathname.replace(/^\//, ''));
  } catch (e) {
    return url.replace(/^\//, '');
  }
}

async function procesarDirecto() {
  const pid = process.pid;
  console.log(`[Perito Directo PID:${pid}] Iniciado en modo estricto de 20s...`);

  while (true) {
    let item = null;

    try {
      const res = await pool.query(`
        SELECT hash_imagen, url_imagen, instancia
        FROM registros_raw
        WHERE estado IN ('PENDIENTE', 'EN_COLA') AND conteo >= 2
        LIMIT 1
        FOR UPDATE SKIP LOCKED;
      `);

      if (res.rows.length > 0) item = res.rows[0];
    } catch (err) {
      console.error(`[PID:${pid} Error DB Query]`, err.message);
    }

    if (!item) {
      await sleep(5000);
      continue;
    }

    try {
      const hora = new Date().toLocaleTimeString();
      console.log(`[${hora}] [PID:${pid}] [INICIO] Hash: ${item.hash_imagen}`);

      if (!item.url_imagen) {
        await pool.query(`UPDATE registros_raw SET estado = 'FALLO' WHERE hash_imagen = $1`, [item.hash_imagen]);
        throw new Error('URL nula');
      }

      // DESCARGA DIRECTA DE R2 (Sin bloqueos de Cloudflare WAF)
      const keyObjeto = obtenerClaveR2(item.url_imagen);
      const command = new GetObjectCommand({
        Bucket: process.env.R2_BUCKET_NAME || 'remesas-img',
        Key: keyObjeto,
      });

      const s3Response = await s3Client.send(command);
      const byteArray = await s3Response.Body.transformToByteArray();
      const imageBase64 = Buffer.from(byteArray).toString('base64');
      const mimeType = s3Response.ContentType || 'image/jpeg';

      // PROCESAMIENTO CON GEMINI AI
      const activeKey = getActiveKey();
      const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${activeKey}`;
      const prompt = `Analiza este comprobante de pago o transferencia y extrae estrictamente un objeto JSON:
      {
        "monto": number o null,
        "moneda": string o null (ej. "USD", "VES", "PEN", "EUR", "CLP"),
        "banco": string o null,
        "referencia": string o null,
        "titular": string o null
      }`;

      const aiResponse = await axios.post(
        url,
        {
          contents: [{
            parts: [
              { text: prompt },
              { inline_data: { mime_type: mimeType, data: imageBase64 } }
            ]
          }],
          generationConfig: { response_mime_type: "application/json" }
        },
        { timeout: 35000 }
      );

      const textResult = aiResponse.data?.candidates?.[0]?.content?.parts?.[0]?.text;
      const datos = parsearJSONSeguro(textResult);

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
        item.hash_imagen,
        datos.monto || null,
        datos.moneda || null,
        datos.banco || null,
        datos.referencia || null,
        datos.titular || null
      ]);

      await pool.query(`UPDATE registros_raw SET estado = 'PROCESADO' WHERE hash_imagen = $1`, [item.hash_imagen]);
      console.log(`[${new Date().toLocaleTimeString()}] [PID:${pid}] [ÉXITO] ${item.hash_imagen}`);

    } catch (err) {
      const status = err.response?.status || err.$metadata?.httpStatusCode;
      const msg = err.response?.data?.error?.message || err.message;

      if (status === 404 || status === 410 || err.name === 'NoSuchKey') {
        console.error(`[PID:${pid} Imagen No Encontrada/404] Hash ${item.hash_imagen}`);
        await pool.query(`UPDATE registros_raw SET estado = 'FALLO' WHERE hash_imagen = $1`, [item.hash_imagen]);
      } else {
        rotateKey();
        console.warn(`[PID:${pid} Reintento Red/API] ${msg}`);
      }
    } finally {
      console.log(`[${new Date().toLocaleTimeString()}] [PID:${pid}] Pausa obligatoria de 20s...`);
      await sleep(20000);
    }
  }
}

procesarDirecto();
