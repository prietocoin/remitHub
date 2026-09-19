const { Worker, Queue } = require('bullmq');
const Redis = require('ioredis');
const { Pool } = require('pg');

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

// 3. Worker: Escucha la cola-validador
const worker = new Worker('cola-validador', async (job) => {
  const payload = job.data;
  console.log(`[Validador] ⚙️ Trabajo recibido de Redis. Hash: ${payload?.hash_largo || 'DESCONOCIDO'}`);

  try {
    // A. Guardar o incrementar conteo en registros_raw
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
      payload.url_r2 || payload.url_imagen || null
    ]);

    const registro = rows[0];
    console.log(`[Validador] 📊 Estado en DB -> Conteo: ${registro.conteo}x | Estado: ${registro.estado}`);

    // B. Regla de Negocio: Activar solo si alcanza el Binomio (>= 2x)
    if (registro.conteo >= 2 && registro.estado === 'RECIBIDO') {
      console.log(`[Validador] 🚀 Binomio 2x alcanzado para: ${payload.hash_largo}`);

      // Transacción atómica en PostgreSQL
      const client = await pool.connect();
      try {
        await client.query('BEGIN');

        // Insertar/actualizar en la tabla operativa de comprobantes
        await client.query(`
          INSERT INTO comprobantes_raw (hash_largo, instancia, url_r2, estado_ia) 
          VALUES ($1, $2, $3, 'LISTO_PARA_IA')
          ON CONFLICT (hash_largo) DO UPDATE SET 
            url_r2 = COALESCE(EXCLUDED.url_r2, comprobantes_raw.url_r2),
            estado_ia = 'LISTO_PARA_IA';
        `, [
          payload.hash_largo, 
          payload.instancia || 'JAIRO', 
          payload.url_r2 || registro.url_imagen || null
        ]);

        // Cambiar estado en auditoría a EN_COLA para prevenir doble procesamiento
        await client.query(`
          UPDATE registros_raw SET estado = 'EN_COLA' WHERE hash_largo = $1;
        `, [payload.hash_largo]);

        await client.query('COMMIT');
      } catch (dbErr) {
        await client.query('ROLLBACK');
        throw dbErr;
      } finally {
        client.release();
      }

      // C. Despachar a la cola del Extractor
      await colaExtractor.add('extraer-datos', {
        hash_largo: payload.hash_largo,
        instancia: payload.instancia || 'JAIRO',
        key_r2: payload.key_r2,
        url_r2: payload.url_r2 || registro.url_imagen
      }, { removeOnComplete: true });

      console.log(`[Validador] ✅ Evento derivado al Extractor: ${payload.hash_largo}`);
    } else {
      console.log(`[Validador] ⏳ 1x registrado. En espera del segundo impacto para el Hash: ${payload.hash_largo}`);
    }

  } catch (err) {
    console.error(`[Validador ERROR] Falló el procesamiento del Hash ${payload?.hash_largo}:`, err.message);
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
      AND timestamp_msg::bigint < (EXTRACT(EPOCH FROM NOW()) - 172800)
    `);
    if (rowCount > 0) console.log(`[Validador Limpieza] ${rowCount} registros (1x) marcados como CADUCADO.`);
  } catch (error) {
    console.error('[Validador Limpieza ERROR]', error.message);
  }
}, 3600000);

console.log('[Validador] 🟢 Worker activo y en escucha.');
