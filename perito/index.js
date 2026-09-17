const { Pool } = require('pg');
const axios = require('axios');

const pool = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT) || 5432,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

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

async function procesarDirecto() {
  console.log('[Perito Directo] Iniciado en modo 1 a 1 estricto (Pausa de 20s)...');

  while (true) {
    let item = null;

    try {
      // Bloqueo de fila para prevenir lecturas duplicadas
      const res = await pool.query(`
        SELECT hash_imagen, url_imagen, instancia
        FROM registros_raw
        WHERE estado IN ('PENDIENTE', 'EN_COLA') AND conteo >= 2
        LIMIT 1
        FOR UPDATE SKIP LOCKED;
      `);

      if (res.rows.length > 0) item = res.rows[0];
    } catch (err) {
      console.error('[Error DB]', err.message);
    }

    if (!item) {
      await sleep(5000);
      continue;
    }

    const horaInicio = new Date().toLocaleTimeString();
    console.log(`[${horaInicio}] [Inicio] Hash: ${item.hash_imagen}`);

    try {
      if (!item.url_imagen) throw { isImageError: true, message: 'URL nula' };

      // 1. Descarga de imagen
      const imgRes = await axios.get(item.url_imagen, { responseType: 'arraybuffer', timeout: 20000 });
      const imageBase64 = Buffer.from(imgRes.data).toString('base64');
      const mimeType = imgRes.headers['content-type'] || 'image/jpeg';

      // 2. Consulta a Gemini IA
      const activeKey = getActiveKey();
      const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-3.5-flash:generateContent?key=${activeKey}`;
      const prompt = `Analiza este comprobante de pago o transferencia y extrae estrictamente un objeto JSON:
      {
        "monto": number o null,
        "moneda": "USD" | "VES" | "EUR" | null,
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

      // 3. Guardar en BD
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
      
      const horaFin = new Date().toLocaleTimeString();
      console.log(`[${horaFin}] [ÉXITO] Completado: ${item.hash_imagen}`);

    } catch (err) {
      const status = err.response?.status;
      const msg = err.response?.data?.error?.message || err.message;

      if (status === 404 || status === 410 || err.isImageError) {
        console.error(`[Imagen no disponible/404] Hash ${item.hash_imagen}`);
        await pool.query(`UPDATE registros_raw SET estado = 'FALLO' WHERE hash_imagen = $1`, [item.hash_imagen]);
      } else {
        rotateKey();
        console.warn(`[Reintento Red/API HTTP ${status || 'Error'}] ${msg}`);
      }
    }

    // PAUSA OBLIGATORIA Y CRUCIAL DE 20 SEGUNDOS
    console.log(`[${new Date().toLocaleTimeString()}] [Pausa] Esperando 20 segundos...`);
    await sleep(20000);
  }
}

procesarDirecto();
