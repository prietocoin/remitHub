const { Pool } = require('pg');
const { Queue } = require('bullmq');
const Redis = require('ioredis');
const axios = require('axios');

// 1. Pool de PostgreSQL
const pool = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT) || 5432,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

// 2. Conexión Redis
const connection = new Redis({
  host: process.env.REDIS_HOST,
  port: Number(process.env.REDIS_PORT) || 6379,
  password: process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: null,
});

const colaIA = new Queue('cola-analisis-ia', { connection });
let estaProcesando = false;

async function extraerYEncolar() {
  if (estaProcesando) return;
  estaProcesando = true;

  try {
    // A. Caducar huérfanos 1X que superen las 24h sin recibir pareja
    await pool.query(`
      UPDATE registros_raw
      SET estado = 'CADUCADO'
      WHERE timestamp_msg::bigint < (EXTRACT(EPOCH FROM NOW()) - 86400)
        AND conteo = 1
        AND estado = 'PENDIENTE';
    `);

    // B. Seleccionar únicamente binomios 2X (conteo > 1) e incluir hash_imagen
    const { rows: cola } = await pool.query(`
      SELECT hash_largo, hash_imagen, url_imagen, instancia
      FROM registros_raw
      WHERE estado = 'PENDIENTE'
        AND conteo > 1
        AND url_imagen IS NOT NULL 
        AND url_imagen LIKE 'http%'
      LIMIT 10;
    `);

    if (cola.length === 0) return;

    console.log(`[Filtro] Procesando ${cola.length} binomio(s) 2X pendientes...`);

    for (const item of cola) {
      try {
        // C. Descarga directa desde R2 (sin bloquear un cliente de la BD)
        const res = await axios.get(item.url_imagen, {
  responseType: 'arraybuffer',
  timeout: 15000 // Elevado de 5,000ms a 15,000ms
});

        const imageBase64 = Buffer.from(res.data).toString('base64');
        const mimeType = res.headers['content-type'] || 'image/jpeg';

        // D. Publicar en Redis propagando la huella hash_imagen para 'perito'
        await colaIA.add('analizar-comprobante', {
          hash_largo: item.hash_largo,
          hash_imagen: item.hash_imagen,
          url_imagen: item.url_imagen,
          instancia: item.instancia,
          imageBase64,
          mimeType
        }, {
          removeOnComplete: true,
          removeOnFail: 100
        });

        // E. Marcar como EN_COLA utilizando la huella binaria
        await pool.query(
          `UPDATE registros_raw SET estado = 'EN_COLA' WHERE hash_imagen = $1`,
          [item.hash_imagen]
        );

        console.log(`[Filtro OK] Encolado exitoso para perito: ${item.hash_imagen}`);

      } catch (err) {
        console.error(`[Filtro Error] Falló descarga/encolado de ${item.hash_imagen}:`, err.message);
        await pool.query(
          `UPDATE registros_raw SET estado = 'FALLO' WHERE hash_imagen = $1`,
          [item.hash_imagen]
        );
      }
    }

  } catch (error) {
    console.error('[Filtro Fatal Error]:', error.message);
  } finally {
    estaProcesando = false;
  }
}

// Bucle de escaneo cada 5 segundos
setInterval(extraerYEncolar, 5000);
extraerYEncolar();
console.log('[Filtro Service] Escaneando binomios 2X en PostgreSQL...');
