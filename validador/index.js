const { Worker, Queue } = require('bullmq');
const Redis = require('ioredis');
const { Pool } = require('pg');
const axios = require('axios');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

// 1. Conexiones
const pool = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT) || 5432,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

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

// 2. Cola de destino
const colaExtractor = new Queue('cola-extractor', { connection: redisConnection });

// 3. Worker: Escucha la cola-validador
const worker = new Worker('cola-validador', async (job) => {
  const payload = job.data;
  console.log(`[Validador] ⚙️ Trabajo recibido de Redis. Hash: ${payload?.hash_largo || 'DESCONOCIDO'}`);

  try {
    // A. Guardar/Actualizar en registros_raw
    const { rows } = await pool.query(`
      INSERT INTO registros_raw (
        hash_largo, hash_corto, grupo_raw, usuario_raw, nombre_push, 
        caption, timestamp_msg, es_imagen, instancia, url_imagen, conteo, estado
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1, 'RECIBIDO')
      ON CONFLICT (hash_largo) DO UPDATE SET 
        conteo = registros_raw.conteo + 1,
        caption = COALESCE(NULLIF(EXCLUDED.caption, ''), registros_raw.caption),
        es_imagen = EXCLUDED.es_imagen OR registros_raw.es_imagen,
        url_imagen = COALESCE(EXCLUDED.url_imagen, registros_raw.url_imagen)
      RETURNING conteo, url_imagen, estado;
    `, [
      payload.hash_largo,
      payload.hash_corto || '',
      payload.grupo_raw || '',
      payload.usuario_raw || '',
      payload.nombre_push || 'Desconocido',
      payload.caption || '',
      payload.timestamp_msg || Math.floor(Date.now() / 1000),
      Boolean(payload.es_imagen),
      payload.instancia || 'JAIRO',
      payload.url_imagen || null
    ]);

    const registro = rows[0];
    console.log(`[Validador] 📊 Estado en DB -> Conteo: ${registro.conteo}x | Estado: ${registro.estado}`);

    // B. Regla de Negocio: Procesar solo si es Binomio (>= 2x)
    if (registro.conteo >= 2 && registro.estado === 'RECIBIDO' && registro.url_imagen) {
      console.log(`[Validador] 🚀 Binomio 2x detectado. Descargando imagen desde WhatsApp...`);

      // C. Descargar imagen
      const res = await axios.get(registro.url_imagen, { responseType: 'arraybuffer', timeout: 15000 });
      const buffer = Buffer.from(res.data);
      const mimeType = res.headers['content-type'] || 'image/jpeg';
      const ext = mimeType.split('/')[1] || 'jpg';
      const keyObjeto = `comprobantes/${payload.hash_largo}.${ext}`;

      // D. Subir a Cloudflare R2
      console.log(`[Validador] ☁️ Subiendo imagen a Cloudflare R2: ${keyObjeto}`);
      await s3Client.send(new PutObjectCommand({
        Bucket: process.env.R2_BUCKET_NAME || 'remesas-img',
        Key: keyObjeto,
        Body: buffer,
        ContentType: mimeType
      }));

      const urlR2 = `https://${process.env.R2_PUBLIC_DOMAIN}/${keyObjeto}`;

      // E. Transacción SQL: Insertar comprobante y actualizar estado
      await pool.query('BEGIN');
      await pool.query(`
        INSERT INTO comprobantes_raw (hash_largo, instancia, url_r2, estado_ia) 
        VALUES ($1, $2, $3, 'LISTO_PARA_IA')
        ON CONFLICT (hash_largo) DO UPDATE SET url_r2 = EXCLUDED.url_r2;
      `, [payload.hash_largo, payload.instancia || 'JAIRO', urlR2]);

      await pool.query(`
        UPDATE registros_raw SET estado = 'EN_COLA' WHERE hash_largo = $1;
      `, [payload.hash_largo]);
      await pool.query('COMMIT');

      // F. Empujar al Extractor
      await colaExtractor.add('extraer-datos', {
        hash_largo: payload.hash_largo,
        instancia: payload.instancia || 'JAIRO',
        key_r2: keyObjeto
      }, { removeOnComplete: true });

      console.log(`[Validador] ✅ Binomio 2x completado y encolado en "cola-extractor": ${payload.hash_largo}`);
    } else {
      console.log(`[Validador] ⏳ 1x guardado exitosamente en DB, esperando pareja. Hash: ${payload.hash_largo}`);
    }

  } catch (err) {
    await pool.query('ROLLBACK').catch(() => {});
    console.error(`[Validador ERROR] Falló el procesamiento del Hash ${payload?.hash_largo}:`, err.message);
    throw err; // Re-lanzar para que BullMQ registre el fallo en la cola
  }
}, { 
  connection: redisConnection, 
  concurrency: 5 
});

// Eventos globales del Worker para depuración en consola
worker.on('completed', (job) => {
  console.log(`[Validador Evento] 🎉 Trabajo ${job.id} finalizado con éxito.`);
});

worker.on('failed', (job, err) => {
  console.error(`[Validador Evento] ❌ Trabajo ${job?.id} falló. Razón: ${err.message}`);
});

worker.on('error', (err) => {
  console.error('[Validador Error Crítico de Conexión]', err.message);
});

// 4. Cron de Limpieza (Cada 1 hora)
setInterval(async () => {
  try {
    const { rowCount } = await pool.query(`
      UPDATE registros_raw 
      SET estado = 'CADUCADO' 
      WHERE conteo = 1 
      AND estado = 'RECIBIDO'
      AND timestamp_msg::bigint < (EXTRACT(EPOCH FROM NOW()) - 172800)
    `);
    if (rowCount > 0) console.log(`[Validador Limpieza] ${rowCount} registros (1x) caducados.`);
  } catch (error) {
    console.error('[Validador Error Limpieza]', error.message);
  }
}, 3600000);

console.log('[Validador] 🟢 Worker activo y listo. Escuchando "cola-validador"...');
