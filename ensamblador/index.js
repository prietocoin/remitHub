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

// 2. Cola de destino
const colaDistribuidor = new Queue('cola-distribuidor', { connection: redisConnection });

// 3. Worker: Ensamblador de Datos
const worker = new Worker('cola-ensamblador', async (job) => {
  const { hash_largo, instancia, datos_ia } = job.data;
  const datos = datos_ia || {};

  console.log(`[Ensamblador] 🧱 Asentando datos extraídos por IA para Hash: ${hash_largo}`);

  const cliente = await pool.connect();

  try {
    await cliente.query('BEGIN');

    // A. Actualiza la tabla de trabajo con los datos extraídos por la IA
    await cliente.query(`
      UPDATE comprobantes_raw SET 
        monto = $1, 
        moneda = $2, 
        banco = $3, 
        referencia = $4, 
        titular = $5, 
        estado_ia = 'PROCESADO',
        procesado_ia = true
      WHERE hash_largo = $6
    `, [
      datos.monto !== undefined ? datos.monto : null,
      datos.moneda || null,
      datos.banco || null,
      datos.referencia || null,
      datos.titular || null,
      hash_largo
    ]);

    // B. Cierra el ciclo en la tabla inmutable
    await cliente.query(`
      UPDATE registros_raw 
      SET estado = 'PROCESADO' 
      WHERE hash_largo = $1
    `, [hash_largo]);

    await cliente.query('COMMIT');
    console.log(`[Ensamblador] 💾 PostgreSQL actualizado con éxito para Hash: ${hash_largo}`);

    // C. Empujar el paquete terminado al Distribuidor
    await colaDistribuidor.add('distribuir-evento', {
      hash_largo,
      instancia: instancia || 'JAIRO',
      datos_finales: datos
    }, { removeOnComplete: true });

    console.log(`[Ensamblador] 🚀 Comprobante ${hash_largo} transferido a "cola-distribuidor".`);

  } catch (error) {
    await cliente.query('ROLLBACK').catch(() => {});
    console.error(`[Ensamblador ERROR] Falló el asentamiento para Hash ${hash_largo}:`, error.message);
    
    // Marcar estado de fallo en la base de datos
    await pool.query(`UPDATE registros_raw SET estado = 'FALLO' WHERE hash_largo = $1`, [hash_largo]).catch(() => {});
    throw error;
  } finally {
    cliente.release();
  }
}, { connection: redisConnection, concurrency: 10 });

// Escuchadores globales de eventos para la consola
worker.on('completed', (job) => {
  console.log(`[Ensamblador Evento] 🎉 Trabajo ${job.id} ensamblado y finalizado exitosamente.`);
});

worker.on('failed', (job, err) => {
  console.error(`[Ensamblador Evento] ❌ Trabajo ${job?.id} falló:`, err.message);
});

worker.on('error', (err) => {
  console.error('[Ensamblador Error de Red/Redis]', err.message);
});

console.log('[Ensamblador] 🟢 Worker activo y listo. Escuchando "cola-ensamblador"...');
