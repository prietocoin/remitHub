const { Pool } = require('pg');
const axios = require('axios');

const pool = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT) || 5432,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

const GEMINI_API_KEY = process.env.GEMINI_API_KEY || '';
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function procesarDirecto() {
  console.log('[Perito Directo] Iniciado sin Redis. Escuchando PostgreSQL...');

  while (true) {
    let client;
    try {
      client = await pool.connect();

      // 1. Tomar 1 registro pendiente sin bloquear la tabla
      const res = await client.query(`
        SELECT hash_imagen, imagebase64, mimetype, instancia
        FROM registros_raw
        WHERE estado = 'PENDIENTE' AND conteo >= 2
        ORDER BY creado_en ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED;
      `);

      if (res.rows.length === 0) {
        client.release();
        await sleep(5000); // Espera 5 segundos si no hay registros pendientes
        continue;
      }

      const item = res.rows[0];
      console.log(`[Procesando] Hash: ${item.hash_imagen}`);

      // 2. Extracción con Gemini 3.5 Flash
      const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-3.5-flash:generateContent?key=${GEMINI_API_KEY}`;
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
              { inline_data: { mime_type: item.mimetype || 'image/jpeg', data: item.imagebase64 } }
            ]
          }],
          generationConfig: { response_mime_type: "application/json" }
        },
        { timeout: 30000 }
      );

      const textResult = aiResponse.data?.candidates?.[0]?.content?.parts?.[0]?.text;
      const datos = JSON.parse(textResult || '{}');

      // 3. Enriquecer comprobantes_raw usando hash_imagen
      await client.query(`
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

      // 4. Marcar como PROCESADO
      await client.query(
        `UPDATE registros_raw SET estado = 'PROCESADO' WHERE hash_imagen = $1`,
        [item.hash_imagen]
      );

      console.log(`[OK] Registrado en comprobantes_raw: ${item.hash_imagen}`);

    } catch (err) {
      const msg = err.response?.data?.error?.message || err.message;
      console.warn(`[Aviso] Falla en llamada/red: ${msg}. Se reintentará en el siguiente ciclo.`);
      // No actualiza estado a 'FALLO': se mantiene en 'PENDIENTE' en PostgreSQL
    } finally {
      if (client) client.release();
    }

    // Pausa de 12s para cumplir cuota Free Tier sin saturar a Google
    await sleep(12000);
  }
}

procesarDirecto();
