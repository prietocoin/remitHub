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
  password: process.env.REDIS_PASSWORD,
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

// 2. Cola de destino (Siguiente módulo)
const colaExtractor = new Queue('cola-extractor', { connection: redisConnection });

// 3. Worker: Escucha eventos 1 a 1 desde Ingesta
const worker = new Worker('cola-validador', async (job) => {
  const payload = job.data;

  // A. Guardar/Actualizar en la tabla inmutable y obtener el estado resultante
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
    payload.hash_largo, payload.hash_corto, payload.grupo_raw, payload.usuario_raw,
    payload.nombre_push, payload.caption, payload.timestamp_msg, payload.es_imagen,
    payload.instancia, payload.url_imagen
  ]);

  const registro = rows[0];

  // B. Regla de Negocio: Solo procesar binomios (2x) que no hayan sido enviados antes
  if (registro.conteo >= 2 && registro.estado === 'RECIBIDO' && registro.url_imagen) {
    
    // C. Descargar de WhatsApp (Origen HTTP)
    const res = await axios.get(registro.url_imagen, { responseType: 'arraybuffer', timeout: 15000 });
    const buffer = Buffer.from(res.data);
    const mimeType = res.headers['content-type'] || 'image/jpeg';
    const keyObjeto = `comprobantes/${payload.hash_largo}.${mimeType.split('/')[1] || 'jpg'}`;

    // D. Subir a Cloudflare R2 de forma privada (SDK S3)
    await s3Client.send(new PutObjectCommand({
      Bucket: process.env.R2_BUCKET_NAME || 'remesas-img',
      Key: keyObjeto,
      Body: buffer,
      ContentType: mimeType
    }));

    const urlR2 = `https://${process.env.R2_PUBLIC_DOMAIN}/${keyObjeto}`; // O mantener solo el Key

    // E. Crear el registro en la tabla de trabajo y actualizar estado original
    await pool.query(`
      INSERT INTO comprobantes_raw (hash_largo, instancia, url_r2, estado_ia) 
      VALUES ($1, $2, $3, 'LISTO_PARA_IA');
      
      UPDATE registros_raw SET estado = 'EN_COLA' WHERE hash_largo = $1;
    `, [payload.hash_largo, payload.instancia, urlR2]);

    // F. Empujar al Extractor (Solo viaja texto, nada de Base64)
    await colaExtractor.add('extraer-datos', {
      hash_largo: payload.hash_largo,
      instancia: payload.instancia,
      key_r2: keyObjeto
    }, { removeOnComplete: true });

    console.log(`[Validador] Binomio 2x completado y encolado: ${payload.hash_largo}`);
  } else {
    // Si es 1x, simplemente termina el trabajo. El dato ya quedó guardado esperando su pareja.
    console.log(`[Validador] 1x guardado, esperando pareja: ${payload.hash_largo}`);
  }
}, { connection: redisConnection, concurrency: 5 });

// 4. Cron de Limpieza (Descarta 1x tras 48 horas)
setInterval(async () => {
  try {
    const { rowCount } = await pool.query(`
      UPDATE registros_raw 
      SET estado = 'CADUCADO' 
      WHERE conteo = 1 
      AND estado = 'RECIBIDO'
      AND timestamp_msg::bigint < (EXTRACT(EPOCH FROM NOW()) - 172800) -- 48 horas en segundos
    `);
    if (rowCount > 0) console.log(`[Validador] Limpieza: ${rowCount} huérfanos (1x) caducados.`);
  } catch (error) {
    console.error('[Validador Error Limpieza]', error.message);
  }
}, 3600000); // Ejecutar cada 1 hora

console.log('[Validador] Escuchando cola y esperando binomios 2x...');
