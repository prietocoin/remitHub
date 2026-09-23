const pool = require('../../../config/db');

async function procesarEnsambladoDatos(payload) {
  const { hash_largo, instancia, datos_ia } = payload;
  const datos = datos_ia || {};
  const hashCorto = hash_largo ? hash_largo.slice(-8) : 'DESCONOCIDO';

  console.log(`[Ensamblador] 🧱 Asentando datos extraídos por IA para Hash: ${hashCorto}`);

  const cliente = await pool.connect();

  try {
    await cliente.query('BEGIN');

    // A. Actualiza comprobantes_raw
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

    // B. Cierra el ciclo en registros_raw
    await cliente.query(`
      UPDATE registros_raw 
      SET estado = 'PROCESADO' 
      WHERE hash_largo = $1
    `, [hash_largo]);

    await cliente.query('COMMIT');
    console.log(`[Ensamblador] 💾 PostgreSQL actualizado con éxito para Hash: ${hashCorto}`);

    return {
      hash_largo,
      instancia: instancia || 'JAIRO',
      datos_finales: datos
    };

  } catch (error) {
    await cliente.query('ROLLBACK').catch(() => {});
    console.error(`[Ensamblador ERROR] Falló el asentamiento para Hash ${hashCorto}:`, error.message);

    await pool.query(`UPDATE registros_raw SET estado = 'FALLO' WHERE hash_largo = $1`, [hash_largo]).catch(() => {});
    throw error;
  } finally {
    cliente.release();
  }
}

module.exports = { procesarEnsambladoDatos };
