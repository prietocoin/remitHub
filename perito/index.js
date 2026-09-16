const { Worker } = require('bullmq');
const Redis = require('ioredis');
const { Pool } = require('pg');
const axios = require('axios');
const FormData = require('form-data');
const crypto = require('crypto');

// 1. Configuración de Variables Globales
const RAW_EVO_URL = process.env.EVOLUTION_URL || 'https://evo.jairokov.com';
const EVOLUTION_URL = RAW_EVO_URL.replace(/\/$/, '');

const EVOLUTION_APIKEY = 
  process.env.EVOLUTION_APIKEY || 
  process.env.EVOLUTION_API_KEY || 
  process.env.API_KEY || 
  process.env.AUTHENTICATION_API_KEY || 
  '';

console.log(`[Worker Init] Target URL: ${EVOLUTION_URL}`);
console.log(`[Worker Init] APIKey detectada: ${EVOLUTION_APIKEY ? 'SI (Cargada)' : 'NO (Vacía)'}`);

// 2. Conexión a PostgreSQL
const pool = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT) || 5432,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

// 3. Conexión a Redis
const connection = new Redis({
  host: process.env.REDIS_HOST,
  port: Number(process.env.REDIS_PORT) || 6379,
  password: process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: null,
});

// 4. Worker Procesador (Buzón / Escritor)
const worker = new Worker('cola-escritor-atom', async (job) => {
  const {
    hash_corto, hash_largo, grupo_raw, usuario_raw,
    nombre_push, caption, timestamp_msg, es_imagen, instance
  } = job.data;

  let urlR2 = null;
  let hash_imagen = hash_largo; // Fallback para mensajes sin imagen

  // Procesamiento de Imagen (si aplica)
  if (es_imagen) {
    try {
      const targetInstance = (instance || 'default').trim();
      const endpoint = `${EVOLUTION_URL}/chat/getBase64FromMediaMessage/${targetInstance}`;

      console.log(`[Worker Media] Solicitando imagen para ${hash_corto} (Instancia: ${targetInstance})`);

      const resMedia = await axios.post(
        endpoint,
        {
          message: { key: { id: hash_largo } },
          convertToMp4: false
        },
        {
          headers: {
            'apikey': EVOLUTION_APIKEY,
            'apiKey': EVOLUTION_APIKEY
          },
          timeout: 15000
        }
      );

      const base64Data = resMedia.data?.base64 || resMedia.data?.mediaBase64;

      if (typeof base64Data === 'string' && base64Data.length > 0) {
        const bufferImagen = Buffer.from(base64Data, 'base64');
        
        // Huella única basada en el binario real de la foto (MD5)
        hash_imagen = crypto.createHash('md5').update(bufferImagen).digest('hex');

        const form = new FormData();
        form.append('file', bufferImagen, `${hash_corto}.jpg`);

        await axios.post('https://api.jairokov.com/upload', form, {
          headers: { ...form.getHeaders() },
          timeout: 15000
        });

        urlR2 = `https://pub-49b9c87f6e6a418ba42de5ba36ddc73e.r2.dev/${hash_corto}.jpg`;
        console.log(`[Worker Media OK] Subida a R2 exitosa: ${urlR2}`);
      } else {
        console.warn(`[Worker Media Warning] Base64 vacío o inválido para ${hash_corto}`);
      }
    } catch (err) {
      console.error(`[Worker Media Error ${hash_corto}]`, {
        status: err.response?.status,
        detalle: err.response?.data || err.message,
        url_intentada: err.config?.url
      });
    }
  }

  // 5. Inserción / Actualización en PostgreSQL (UPSERT por hash_imagen)
  // Estado se mantiene en 'PENDIENTE' para que 'filtro' pueda escanear binomios 2X
  const queryUpsert = `
    INSERT INTO registros_raw (
      hash_corto, hash_largo, grupo_raw, usuario_raw, nombre_push,
      caption, timestamp_msg, url_imagen, conteo, estado, instancia, hash_imagen
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1, 'PENDIENTE', $9, $10)
    ON CONFLICT (hash_imagen) DO UPDATE SET
      conteo = registros_raw.conteo + 1,
      grupo_raw_2 = CASE WHEN registros_raw.grupo_raw <> EXCLUDED.grupo_raw THEN EXCLUDED.grupo_raw ELSE registros_raw.grupo_raw_2 END,
      usuario_raw_2 = CASE WHEN registros_raw.usuario_raw <> EXCLUDED.usuario_raw THEN EXCLUDED.usuario_raw ELSE registros_raw.usuario_raw_2 END,
      url_imagen = COALESCE(EXCLUDED.url_imagen, registros_raw.url_imagen),
      timestamp_msg = EXCLUDED.timestamp_msg,
      instancia = COALESCE(EXCLUDED.instancia, registros_raw.instancia),
      estado = 'PENDIENTE'
    RETURNING (xmax = 0) AS es_nuevo, hash_corto, conteo;
  `;

  const values = [
    hash_corto, 
    hash_largo, 
    grupo_raw, 
    usuario_raw, 
    nombre_push, 
    caption, 
    timestamp_msg, 
    urlR2, 
    instance || 'default',
    hash_imagen
  ];

  const result = await pool.query(queryUpsert, values);

  console.log(`[Buzón OK] Ingesta: ${hash_corto} | Conteo: ${result.rows[0].conteo} | Es nuevo: ${result.rows[0].es_nuevo}`);
  return result.rows[0];
}, { connection });

// 6. Manejo de Errores Globales
worker.on('failed', (job, err) => {
  console.error(`[Worker Job Error] Tarea ${job?.data?.hash_corto} falló:`, err.message);
});

worker.on('error', (err) => {
  console.error('[Worker Fatal Error]', err.message);
});

console.log('[escritor-worker] Escuchando la cola de Redis...');
