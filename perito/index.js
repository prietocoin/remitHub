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

async function procesarDirecto() {
  console.log('[Perito Directo] Iniciado. Escuchando PostgreSQL...');

  while (true) {
    let item = null;

    // 1. Obtener registro de la BD y liberar cliente inmediatamente
    try {
      const res = await pool.query(`
        SELECT hash_imagen, url_imagen, instancia
        FROM registros_raw
        WHERE estado = 'PENDIENTE' AND conteo >= 2
        LIMIT 1;
      `);

      if (res.rows.length > 0) {
        item = res.rows[0];
      }
    } catch (err) {
      console.error('[Error Consulta DB]', err.message);
    }

    // Si no hay pendientes, pausar 5s y continuar
    if (!item) {
      await sleep(5000);
      continue;
    }

    // 2. Procesar imagen y consultar Gemini IA
    try {
      console.log(`[Procesando] Hash: ${item.hash_imagen}`);

      const imgRes = await axios.get(item.url_imagen, { responseType: 'arraybuffer', timeout: 15000 });
      const imageBase64 = Buffer.from(imgRes.data).toString('base64');
      const mimeType = imgRes.headers['content-type'] || 'image/jpeg';

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
        { timeout: 30000 }
      );

      const textResult = aiResponse.data?.candidates?.[0]?.content?.parts?.[0]?.text;
      const datos = JSON.parse(textResult || '{}');

      // 3. Insertar/Actualizar comprobantes_raw
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

      // 4. Marcar registro como PROCESADO
      await pool.query(
        `UPDATE registros_raw SET estado = 'PROCESADO' WHERE hash_imagen = $1`,
        [item.hash_imagen]
      );

      console.log(`[OK] Guardado en comprobantes_raw: ${item.hash_imagen}`);

    } catch (err) {
      const status = err.response?.status;
      const msg = err.response?.data?.error?.message || err.message;

      if (status === 429 || status >= 500) {
        rotateKey();
        console.warn(`[Gemini Reintento ${status || 'Red'}] ${msg}. Reintentando en el siguiente ciclo...`);
      } else {
        console.error(`[Error Procesamiento] ${msg}`);
      }
    }

    // Pausa de 12 segundos entre peticiones
    await sleep(12000);
  }
}

procesarDirecto();
