const { Queue } = require('bullmq');
const pool = require('../config/db');
const redisConfig = require('../config/redis');
const { resolverUrlImagen } = require('../utils/imageResolver');

// Instanciar la cola extractor para reprocesamiento
const colaExtractor = new Queue('cola-extractor', { connection: redisConfig });

async function getInstancias(req, res) {
  try {
    // Consulta directa a impactos_raw para reflejar instancias desde el primer webhook
    const { rows } = await pool.query(`
      SELECT DISTINCT LOWER(instancia) as instancia 
      FROM impactos_raw 
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

    const itemsFormateados = rows.map(row => {
      const url1 = resolverUrlImagen(row.url_imagen_1 || row.url_r2_comprobante, row.hash_largo);
      let url2 = row.url_imagen_2 ? resolverUrlImagen(row.url_imagen_2, row.hash_largo + '_2') : null;

      // Deduplicación en backend: Si apunta a la misma imagen, se anula url2
      if (url2 === url1) {
        url2 = null;
      }

      return {
        ...row,
        url_imagen_1: url1,
        url_imagen_2: url2
      };
    });

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

async function releerIA(req, res) {
  try {
    const { hash } = req.params;

    // 1. Buscar imagen cruzando impactos_raw y comprobantes_raw
    const { rows } = await pool.query(
      `SELECT 
         i.hash_largo, 
         COALESCE(c.url_r2, i.url_imagen) as url_r2, 
         COALESCE(c.instancia, i.instancia) as instancia,
         i.caption
       FROM impactos_raw i
       LEFT JOIN comprobantes_raw c ON i.hash_largo = c.hash_largo
       WHERE i.hash_largo = $1 OR c.hash_largo = $1
       LIMIT 1`,
      [hash]
    );

    if (rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Comprobante no encontrado en BD' });
    }

    const comprobante = rows[0];

    // 2. Crear o actualizar estado en comprobantes_raw (UPSERT)
    await pool.query(
      `INSERT INTO comprobantes_raw (hash_largo, url_r2, instancia, estado_ia, procesado_ia)
       VALUES ($1, $2, $3, 'RE-PROCESANDO', false)
       ON CONFLICT (hash_largo) 
       DO UPDATE SET estado_ia = 'RE-PROCESANDO', procesado_ia = false`,
      [comprobante.hash_largo, comprobante.url_r2, comprobante.instancia]
    );

    // 3. Enviar a la cola de BullMQ para el microservicio extractor
    await colaExtractor.add('extraer-datos', {
      hash_largo: comprobante.hash_largo,
      url_r2: comprobante.url_r2,
      instancia: comprobante.instancia,
      caption: comprobante.caption
    }, {
      attempts: 3,
      removeOnComplete: true
    });

    return res.json({ success: true, message: 'Re-lectura enviada a la cola' });
  } catch (err) {
    console.error('Error al solicitar re-lectura IA:', err.message);
    return res.status(500).json({ success: false, error: err.message });
  }
}

module.exports = {
  getInstancias,
  getComprobantes,
  deleteComprobante,
  releerIA
};
