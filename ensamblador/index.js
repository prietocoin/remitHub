const { Worker } = require('bullmq');
const pool = require('./src/config/db');
const redisConfig = require('./src/config/redis');
const colaDistribuidor = require('./src/queues/distribuidor.queue');

const worker = new Worker('cola-ensamblador', async (job) => {
  const { hash_largo, instancia, datos_ia } = job.data;
  const datos = datos_ia || {};
  const hashCorto = hash_largo ? hash_largo.slice(-8) : 'DESCONOCIDO';

  console.log(`[Ensamblador] 🧱 Asentando datos extraídos por IA para Hash: ${hashCorto}`);

  const cliente = await pool.connect();

  try {
    await cliente.query('BEGIN');

    // A. Actualiza la tabla operativa con los datos extraídos por la IA
   await cliente.query(`
  INSERT INTO comprobantes_raw (
    hash_largo, monto, moneda, banco, referencia, titular, estado_ia, procesado_ia
  ) VALUES ($1, $2, $3, $4, $5, $6, 'PROCESADO', true)
  ON CONFLICT (hash_largo) DO UPDATE SET
    monto = EXCLUDED.monto,
    moneda = EXCLUDED.moneda,
    banco = EXCLUDED.banco,
    referencia = EXCLUDED.referencia,
    titular = EXCLUDED.titular,
    estado_ia = 'PROCESADO',
    procesado_ia = true;
`, [
  hash_largo,
  datos.monto !== undefined ? datos.monto : null,
  datos.moneda || null,
  datos.banco || null,
  datos.referencia || null,
  datos.titular || null
]);

    // B. Cierra el ciclo en la tabla inmutable de auditoría
    await cliente.query(`
      UPDATE registros_raw 
      SET estado = 'PROCESADO' 
      WHERE hash_largo = $1
    `, [hash_largo]);

    await cliente.query('COMMIT');
    console.log(`[Ensamblador] 💾 PostgreSQL actualizado con éxito para Hash: ${hashCorto}`);

    // C. Empujar el paquete terminado al Distribuidor
    await colaDistribuidor.add('distribuir-evento', {
      hash_largo,
      instancia: instancia || 'JAIRO',
      datos_finales: datos
    });

    console.log(`[Ensamblador] 🚀 Comprobante ${hashCorto} transferido a "cola-distribuidor".`);

  } catch (error) {
    await cliente.query('ROLLBACK').catch(() => {});
    console.error(`[Ensamblador ERROR] Falló el asentamiento para Hash ${hashCorto}:`, error.message);
    
    // Marcar estado de fallo en la base de datos
    await pool.query(`UPDATE registros_raw SET estado = 'FALLO' WHERE hash_largo = $1`, [hash_largo]).catch(() => {});
    throw error;
  } finally {
    cliente.release();
  }
}, { 
  connection: redisConfig, 
  concurrency: 10 
});

// Listener de eventos
worker.on('completed', (job) => {
  console.log(`[Ensamblador Evento] 🎉 Job ${job.id} ensamblado y finalizado exitosamente.`);
});

worker.on('failed', (job, err) => {
  console.error(`[Ensamblador Evento] ❌ Job ${job?.id} falló:`, err.message);
});

worker.on('error', (err) => {
  console.error('[Ensamblador Error de Red/Redis]', err.message);
});

console.log('[Ensamblador] 🟢 Worker activo y listo. Escuchando "cola-ensamblador"...');
