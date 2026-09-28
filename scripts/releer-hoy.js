const pool = require('../src/config/db');
const { Queue } = require('bullmq');
const redisConfig = require('../src/config/redis');

// Cola unificada del pipeline de Remit Hub
const pipelineQueue = new Queue('cola-pipeline', { connection: redisConfig });

async function releerComprobantesHoy() {
  try {
    // 1. Obtener todos los comprobantes generados el día de hoy
    const { rows } = await pool.query(`
      SELECT DISTINCT ON (c.hash_largo)
        c.hash_largo, 
        COALESCE(c.url_r2, i.url_imagen) AS url_r2, 
        COALESCE(c.instancia, i.instancia, 'JAIRO') AS instancia, 
        i.caption
      FROM comprobantes_raw c
      LEFT JOIN impactos_raw i ON LOWER(TRIM(c.hash_largo)) = LOWER(TRIM(i.hash_largo))
      WHERE c.creado_en >= CURRENT_DATE
      ORDER BY c.hash_largo, c.creado_en DESC;
    `);

    if (rows.length === 0) {
      console.log('ℹ️ No se encontraron comprobantes registrados el día de hoy.');
      process.exit(0);
    }

    console.log(`📦 Encontrados ${rows.length} comprobantes de hoy. Encolando para re-lectura IA...`);

    // 2. Marcar cada comprobante como 'RE-PROCESANDO' y enviarlo a la cola de BullMQ
    for (const item of rows) {
      await pool.query(`
        UPDATE comprobantes_raw 
        SET estado_ia = 'RE-PROCESANDO', procesado_ia = false 
        WHERE LOWER(TRIM(hash_largo)) = LOWER(TRIM($1));
      `, [item.hash_largo]);

      await pipelineQueue.add('releer-ia', {
        hash_largo: item.hash_largo,
        url_r2: item.url_r2,
        instancia: item.instancia || 'JAIRO',
        caption: item.caption
      }, {
        attempts: 3,
        removeOnComplete: true
      });

      console.log(`⚡ Re-lectura encolada para Hash: ${item.hash_largo.substring(0, 8)}`);
    }

    console.log('✅ Todos los comprobantes de hoy han sido enviados a la cola-pipeline.');
  } catch (err) {
    console.error('❌ Error al ejecutar re-lectura:', err.message);
  } finally {
    await pipelineQueue.close();
    await pool.end();
    process.exit(0);
  }
}

releerComprobantesHoy();
