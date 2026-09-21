const { Worker, Queue } = require('bullmq');
const Redis = require('ioredis');
const { Pool } = require('pg');
const { PutObjectCommand } = require('@aws-sdk/client-s3');

const s3Client = require('./src/config/r2');
const { obtenerBufferImagen } = require('./src/services/evolution');

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

// 2. Cola de destino hacia el Extractor
const colaExtractor = new Queue('cola-extractor', { connection: redisConnection });

// Helper: Descarga desde Evolution API y sube a Cloudflare R2
async function asegurarImagenEnR2(hashLargo, rawPayload, instancia) {
  // A. Verificar si ya existe en R2 (Protegido contra errores SQL)
  try {
    const checkDb = await pool.query(
      `SELECT url_imagen FROM impactos_raw WHERE hash_largo = $1 AND url_imagen IS NOT NULL LIMIT 1`,
      [hashLargo]
    );

    if (checkDb.rows.length > 0 && checkDb.rows[0].url_imagen) {
      return checkDb.rows[0].url_imagen;
    }
  } catch (err) {
    console.warn(`[Validador ⚠️] Verificación en impactos_raw omitida: ${err.message}`);
  }

  // B. Extraer objeto de mensaje con resolución flexible de propiedades
  const item = Array.isArray(rawPayload) ? rawPayload[0] : rawPayload;
  const body = item?.body || item || {};
  const data = body?.data || item?.data || body || {};
  const key = data?.key || item?.key || body?.key || {};
  const message = data?.message || item?.message || body?.message || {};

  console.log(`[Validador] ☁️ Descargando imagen desde Evolution API para Hash: ${hashLargo.slice(-8)}...`);
  const imageBuffer = await obtenerBufferImagen(instancia, data?.instanceId || item?.instanceId, key, message);

  if (!imageBuffer) {
    console.error(`[Validador ⚠️] No se pudo obtener el buffer de la imagen para Hash: ${hashLargo}`);
    return null;
  }

  const keyR2 = `comprobantes/${hashLargo}.jpg`;
  const bucketName = process.env.R2_BUCKET_NAME || 'remesas-img';

  await s3Client.send(new PutObjectCommand({
    Bucket: bucketName,
    Key: keyR2,
    Body: imageBuffer,
    ContentType: 'image/jpeg',
  }));

  const publicDomain = process.env.R2_PUBLIC_DOMAIN || '';
  const urlR2 = publicDomain ? `${publicDomain}/${keyR2}` : keyR2;

  console.log(`[Validador] ✅ Imagen guardada en R2: ${urlR2}`);
  return urlR2;
}

// 3. Worker: Escucha la cola-validador
const worker = new Worker('cola-validador', async (job) => {
  const payload = job.data;

  // Normalización de Hash (soporta snake_case y camelCase)
  const hashLargo = payload.hash_largo || payload.hashLargo;
  const hashCorto = payload.hash_corto || payload.hashCorto || (hashLargo ? hashLargo.slice(-8) : null);

  if (!hashLargo) {
    console.error(`[Validador ❌] Job ${job.id} rechazado: No se recibió hash_largo en el payload.`, payload);
    throw new Error('Payload inválido: falta hash_largo');
  }

  const { 
    impactoId, instancia, usuarioRaw, grupoRaw, 
    nombrePush, caption, timestampMsg, rawPayload 
  } = payload;

  console.log(`[Validador] ⚙️ Procesando Impacto #${impactoId || 'N/A'} | Hash: ${hashCorto}`);

  try {
    // A. Garantizar la presencia de la imagen en R2
    const urlR2 = await asegurarImagenEnR2(hashLargo, rawPayload, instancia);

    if (urlR2 && impactoId) {
      await pool.query(`UPDATE impactos_raw SET url_imagen = $1 WHERE id = $2`, [urlR2, impactoId]).catch(err => {
        console.warn(`[Validador ⚠️] No se actualizó url_imagen en impactos_raw: ${err.message}`);
      });
    }

    // B. Actualizar o Insertar en registros_raw (Conteo acumulativo)
    const { rows } = await pool.query(`
      INSERT INTO registros_raw (
        hash_largo, hash_corto, grupo_raw, usuario_raw, nombre_push, 
        caption, timestamp_msg, es_imagen, instancia, url_imagen, conteo, estado
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, true, $8, $9, 1, 'RECIBIDO')
      ON CONFLICT (hash_largo) DO UPDATE SET 
        conteo = registros_raw.conteo + 1,
        caption = COALESCE(NULLIF(EXCLUDED.caption, ''), registros_raw.caption),
        url_imagen = COALESCE(EXCLUDED.url_imagen, registros_raw.url_imagen)
      RETURNING conteo, url_imagen, estado;
    `, [
      hashLargo,
      hashCorto,
      grupoRaw || '',
      usuarioRaw || '',
      nombrePush || 'Desconocido',
      caption || '',
      timestampMsg || Math.floor(Date.now() / 1000),
      instancia || 'JAIRO',
      urlR2
    ]);

    const registro = rows[0];
    console.log(`[Validador] 📊 Estado en DB -> Conteo: ${registro.conteo}x | Estado: ${registro.estado}`);

    // C. Regla de Negocio: Activar solo al alcanzar el Binomio (>= 2x)
    if (registro.conteo >= 2 && registro.estado === 'RECIBIDO') {
      console.log(`[Validador] 🚀 Binomio 2x alcanzado para Hash: ${hashCorto}`);

      const client = await pool.connect();
      try {
        await client.query('BEGIN');

        // 1. Asentar registro base en comprobantes_raw
        await client.query(`
          INSERT INTO comprobantes_raw (hash_largo, instancia, url_r2, estado_ia) 
          VALUES ($1, $2, $3, 'LISTO_PARA_IA')
          ON CONFLICT (hash_largo) DO UPDATE SET 
            url_r2 = COALESCE(EXCLUDED.url_r2, comprobantes_raw.url_r2),
            estado_ia = 'LISTO_PARA_IA';
        `, [
          hashLargo, 
          instancia || 'JAIRO', 
          urlR2 || registro.url_imagen || null
        ]);

        // 2. Despachar a Redis ANTES de cerrar la transacción
        await colaExtractor.add('extraer-datos', {
          hash_largo: hashLargo,
          instancia: instancia || 'JAIRO',
          url_r2: urlR2 || registro.url_imagen
        }, { removeOnComplete: true });

        // 3. Actualizar estado en registros_raw
        await client.query(`
          UPDATE registros_raw SET estado = 'EN_COLA' WHERE hash_largo = $1;
        `, [hashLargo]);

        await client.query('COMMIT');
        console.log(`[Validador] ✅ Evento derivado al Extractor: ${hashCorto}`);

      } catch (dbErr) {
        await client.query('ROLLBACK');
        throw dbErr; // Forzar reintento en BullMQ si falla SQL o Redis
      } finally {
        client.release();
      }
    } else {
      console.log(`[Validador] ⏳ ${registro.conteo}x registrado. En espera del binomio para Hash: ${hashCorto}`);
    }

  } catch (err) {
    console.error(`[Validador ERROR] Falló el procesamiento del Hash ${hashCorto}:`, err.message);
    throw err;
  }
}, { 
  connection: redisConnection, 
  concurrency: 5 
});

// Eventos de monitoreo
worker.on('completed', (job) => {
  console.log(`[Validador Evento] 🎉 Job ${job.id} procesado.`);
});

worker.on('failed', (job, err) => {
  console.error(`[Validador Evento] ❌ Job ${job?.id} falló: ${err.message}`);
});

worker.on('error', (err) => {
  console.error('[Validador Error Crítico]', err.message);
});

// Cron de Limpieza (Cada 1 hora): Marca caducados los 1x huérfanos tras 48 horas
setInterval(async () => {
  try {
    const { rowCount } = await pool.query(`
      UPDATE registros_raw 
      SET estado = 'CADUCADO' 
      WHERE conteo = 1 
      AND estado = 'RECIBIDO'
      AND NULLIF(timestamp_msg, '')::bigint < (EXTRACT(EPOCH FROM NOW()) - 172800)
    `);
    if (rowCount > 0) console.log(`[Validador Limpieza] ${rowCount} registros (1x) marcados como CADUCADO.`);
  } catch (error) {
    console.error('[Validador Limpieza ERROR]', error.message);
  }
}, 3600000);

console.log('[Validador] 🟢 Worker activo y en escucha.');
