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
      filtroConteo = 'WHERE GREATEST(i.total_impactos, COALESCE(r.conteo, 1)) > 1';
    }

    const query = `
      WITH ranked_impactos AS (
        SELECT 
          id,
          hash_largo,
          instancia,
          usuario_raw,
          grupo_raw,
          nombre_push,
          caption,
          url_imagen,
          timestamp_msg,
          ROW_NUMBER() OVER (PARTITION BY hash_largo ORDER BY id ASC) as num_impacto
        FROM impactos_raw
        WHERE LOWER(instancia) = LOWER($1)
      ),
      impactos_consolidados AS (
        SELECT 
          hash_largo,
          MAX(instancia) as instancia,
          COUNT(*) as total_impactos,
          MAX(timestamp_msg) as timestamp_msg,
          -- Impacto 1 (Primer mensaje real en impactos_raw)
          MAX(NULLIF(nombre_push, '')) FILTER (WHERE num_impacto = 1) as nombre_push_1,
          MAX(NULLIF(usuario_raw, '')) FILTER (WHERE num_impacto = 1) as usuario_raw_1,
          MAX(NULLIF(grupo_raw, '')) FILTER (WHERE num_impacto = 1) as grupo_raw_1,
          MAX(NULLIF(caption, '')) FILTER (WHERE num_impacto = 1) as caption_1,
          MAX(NULLIF(url_imagen, '')) FILTER (WHERE num_impacto = 1) as url_imagen_1,
          -- Impacto 2 (Segundo mensaje real en impactos_raw)
          MAX(NULLIF(nombre_push, '')) FILTER (WHERE num_impacto = 2) as nombre_push_2,
          MAX(NULLIF(usuario_raw, '')) FILTER (WHERE num_impacto = 2) as usuario_raw_2,
          MAX(NULLIF(grupo_raw, '')) FILTER (WHERE num_impacto = 2) as grupo_raw_2,
          MAX(NULLIF(caption, '')) FILTER (WHERE num_impacto = 2) as caption_2,
          MAX(NULLIF(url_imagen, '')) FILTER (WHERE num_impacto = 2) as url_imagen_2
        FROM ranked_impactos
        GROUP BY hash_largo
      )
      SELECT 
        i.hash_largo,
        COALESCE(c.estado_ia, r.estado, 'RECIBIDO') as estado,
        i.timestamp_msg,
        i.instancia,
        GREATEST(i.total_impactos, COALESCE(r.conteo, 1)) as conteo,
        i.nombre_push_1,
        i.usuario_raw_1,
        i.grupo_raw_1,
        i.caption_1,
        i.url_imagen_1,
        i.nombre_push_2,
        i.usuario_raw_2,
        i.grupo_raw_2,
        i.caption_2,
        i.url_imagen_2,
        c.url_r2 as url_r2_comprobante,
        c.monto,
        c.moneda,
        c.banco,
        c.referencia,
        c.titular,
        c.estado_ia
      FROM impactos_consolidados i
      LEFT JOIN registros_raw r ON i.hash_largo = r.hash_largo
      LEFT JOIN comprobantes_raw c ON i.hash_largo = c.hash_largo
      ${filtroConteo}
      ORDER BY i.timestamp_msg DESC
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
