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
  console.log('[Perito Directo] Modo 1 a 1 Estricto (Pausa de 20s) Activado...');

  while (true) {
    let item = null;

    // 1. Tomar ESTRICTAMENTE 1 solo registro
    try {
      const res = await pool.query(`
        SELECT hash_imagen, url_imagen, instancia
        FROM registros_raw
        WHERE estado IN ('PENDIENTE', 'EN_COLA') AND conteo >= 2
        LIMIT 1;
      `);

      if (res.rows.length > 0) item = res.rows[0];
    } catch (err) {
      console.error('[Error Consulta DB]', err.message);
    }

    // Si no hay pendientes, espera 5 segundos y vuelve a consultar
    if (!item) {
      await sleep(5000);
      continue;
    }

    console.log(`[Procesando 1 a 1] Hash: ${item.hash_imagen}`);

    // PASO 1: Descargar imagen con manejo aislado de error
    let imageBase64, mimeType;
    try {
      if (!item.url_imagen) throw { isImageError: true, message: 'URL vacía' };

      const imgRes = await axios.get(item.url_imagen, { responseType: 'arraybuffer', timeout: 20000 });
      imageBase64 = Buffer.from(imgRes.data).toString('base64');
      mimeType = imgRes.headers['content-type'] || 'image/jpeg';
    } catch (imgErr) {
      const imgStatus = imgErr.response?.status;
      console.error(`[Error Descarga Imagen] Hash ${item.hash_imagen}: HTTP ${imgStatus || 'RED'}`);

      // Solo si la imagen NO existe (404/410), se marca como FALLO
      if (imgStatus === 404 || imgStatus === 410 || imgErr.isImageError) {
        await pool.query(`UPDATE registros_raw SET estado = 'FALLO' WHERE hash_imagen = $1`, [item.hash_imagen]);
      }
      // Si fue parpadeo de red, permanece en PENDIENTE y espera 20s
      await sleep(20000);
      continue;
    }

    // PASO 2: Extraer con Gemini e insertar en PostgreSQL
    try {
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
      console.log(`[ÉXITO] ${item.hash_imagen}`);

    } catch (apiErr) {
      rotateKey();
      const status = apiErr.response?.status;
      const detail = apiErr.response?.data?.error?.message || apiErr.message;
      console.warn(`[Reintento API/Red - HTTP ${status || 'Error'}] ${detail}. Se mantiene en cola.`);
    }

    // Pausa estricta de 20 segundos antes de tomar el SIGUIENTE registro
    await sleep(20000);
  }
}

procesarDirecto();
