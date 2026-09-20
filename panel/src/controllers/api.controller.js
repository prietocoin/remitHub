const pool = require('../config/db');
const { resolverUrlImagen } = require('../utils/imageResolver');

async function getInstancias(req, res) {
  try {
    const { rows } = await pool.query(`
      SELECT DISTINCT LOWER(instancia) as instancia 
      FROM registros_raw 
      WHERE instancia IS NOT NULL AND instancia <> ''
      ORDER BY instancia ASC
    `);
    res.json(rows.map(r => r.instancia));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}

async function getComprobantes(req, res) {
  try {
    const instanciaTarget = req.query.instancia || 'JAIRO';
    const soloBinomios = req.query.solo_binomios === 'true';

    let filtroConteo = '';
    if (soloBinomios) {
      filtroConteo = 'WHERE GREATEST(r.total_impactos, r.conteo_max) > 1';
    }

    const query = `
      WITH ranked_raw AS (
        SELECT 
          *,
          ROW_NUMBER() OVER (PARTITION BY hash_largo ORDER BY timestamp_msg ASC, ctid ASC) as num_impacto
        FROM registros_raw
        WHERE LOWER(instancia) = LOWER($1)
      ),
      raw_consolidado AS (
        SELECT 
          hash_largo,
          MAX(instancia) as instancia,
          COUNT(*) as total_impactos,
          MAX(conteo) as conteo_max,
          MAX(timestamp_msg) as timestamp_msg,
          -- Impacto 1 (Primer mensaje recibido)
          MAX(NULLIF(nombre_push, '')) FILTER (WHERE num_impacto = 1) as nombre_push_1,
          MAX(NULLIF(usuario_raw, '')) FILTER (WHERE num_impacto = 1) as usuario_raw_1,
          MAX(NULLIF(grupo_raw, '')) FILTER (WHERE num_impacto = 1) as grupo_raw_1,
          MAX(NULLIF(caption, '')) FILTER (WHERE num_impacto = 1) as caption_1,
          MAX(NULLIF(url_imagen, '')) FILTER (WHERE num_impacto = 1) as url_imagen_1,
          -- Impacto 2 (Segundo mensaje recibido)
          MAX(NULLIF(nombre_push, '')) FILTER (WHERE num_impacto = 2) as nombre_push_2,
          MAX(NULLIF(usuario_raw, '')) FILTER (WHERE num_impacto = 2) as usuario_raw_2,
          MAX(NULLIF(grupo_raw, '')) FILTER (WHERE num_impacto = 2) as grupo_raw_2,
          MAX(NULLIF(caption, '')) FILTER (WHERE num_impacto = 2) as caption_2,
          MAX(NULLIF(url_imagen, '')) FILTER (WHERE num_impacto = 2) as url_imagen_2,
          MAX(estado) as estado_raw
        FROM ranked_raw
        GROUP BY hash_largo
      )
      SELECT 
        r.hash_largo,
        COALESCE(c.estado_ia, CASE WHEN GREATEST(r.total_impactos, r.conteo_max) >= 2 THEN 'PROCESADO' ELSE r.estado_raw END) as estado,
        r.timestamp_msg,
        r.instancia,
        GREATEST(r.total_impactos, r.conteo_max) as conteo,
        r.nombre_push_1,
        r.usuario_raw_1,
        r.grupo_raw_1,
        r.caption_1,
        r.url_imagen_1,
        r.nombre_push_2,
        r.usuario_raw_2,
        r.grupo_raw_2,
        r.caption_2,
        r.url_imagen_2,
        c.url_r2 as url_r2_comprobante,
        c.monto,
        c.moneda,
        c.banco,
        c.referencia,
        c.titular,
        c.estado_ia
      FROM raw_consolidado r
      LEFT JOIN comprobantes_raw c ON r.hash_largo = c.hash_largo
      ${filtroConteo}
      ORDER BY r.timestamp_msg DESC
      LIMIT 60
    `;
    const { rows } = await pool.query(query, [instanciaTarget]);

    const itemsFormateados = rows.map(row => ({
      ...row,
      url_imagen_1: resolverUrlImagen(row.url_imagen_1 || row.url_r2_comprobante, row.hash_largo),
      url_imagen_2: row.url_imagen_2 ? resolverUrlImagen(row.url_imagen_2, row.hash_largo + '_2') : null
    }));

    res.json(itemsFormateados);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}

async function deleteComprobante(req, res) {
  const { hash } = req.params;
  try {
    await pool.query(`UPDATE registros_raw SET estado = 'DESCARTADO' WHERE hash_largo = $1`, [hash]);
    res.json({ success: true, hash });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}

module.exports = {
  getInstancias,
  getComprobantes,
  deleteComprobante
};
