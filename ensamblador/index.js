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
  port: process.env.REDIS_PORT || 6379,
});

// 2. Cola de destino: Puente hacia la Etapa 2 (El Enrutador Dinámico)
const colaDistribuidor = new Queue('cola-distribuidor', { connection: redisConnection });

// 3. Worker: Ensamblador de Datos
const worker = new Worker('cola-ensamblador', async (job) => {
  const { hash_largo, instancia, datos_ia } = job.data;
  console.log(`[Ensamblador] Asentando datos para: ${hash_largo}`);

  try {
    // A. Ejecutar los UPDATES finales en una sola transacción
    const cliente = await pool.connect();
    try {
      await cliente.query('BEGIN');

      // Actualiza la tabla de trabajo con los datos extraídos
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
        datos_ia.monto || null,
        datos_ia.moneda || null,
        datos_ia.banco || null,
        datos_ia.referencia || null,
        datos_ia.titular || null,
        hash_largo
      ]);

      // Cierra el ciclo en la tabla inmutable
      await cliente.query(`
        UPDATE registros_raw 
        SET estado = 'PROCESADO' 
        WHERE hash_largo = $1
      `, [hash_largo]);

      await cliente.query('COMMIT');
    } catch (dbError) {
      await cliente.query('ROLLBACK');
      throw dbError;
    } finally {
      cliente.release();
    }

    // B. Empujar el paquete terminado a la Capa 0 para iniciar las reglas de negocio
    await colaDistribuidor.add('distribuir-evento', {
      hash_largo,
      instancia,
      datos_finales: datos_ia
    }, { removeOnComplete: true });

    console.log(`[Ensamblador] Éxito. Comprobante ${hash_largo} transferido al Distribuidor.`);

  } catch (error) {
    console.error(`[Ensamblador Error] Falló ${hash_largo}:`, error.message);
    
    // Marcar como fallo en BD si algo sale mal
    await pool.query(`UPDATE registros_raw SET estado = 'FALLO' WHERE hash_largo = $1`, [hash_largo]);
    throw error;
  }
}, { connection: redisConnection, concurrency: 10 });

console.log('[Ensamblador] Worker iniciado, esperando resultados de IA...');
