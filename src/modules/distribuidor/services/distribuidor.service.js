const pool = require('../../../config/db');
const { enviarWebhookFinal } = require('./webhook');

function resolverUrlImagen(urlR2) {
  const r2Domain = (process.env.R2_PUBLIC_DOMAIN || '').replace(/\/$/, '');
  if (!urlR2 || !r2Domain) return urlR2 || null;
  return urlR2.startsWith('http') ? urlR2 : `${r2Domain}/${urlR2}`;
}

async function procesarDistribucion(payloadWorker) {
  const { hash_largo, instancia } = payloadWorker;
  const hashCorto = hash_largo ? hash_largo.slice(-8) : 'DESCONOCIDO';

  console.log(`[Distribuidor] 📦 Construyendo Payload Maestro para Hash: ${hashCorto}`);
  const cliente = await pool.connect();

  try {
    // 1. Obtener metadatos de los impactos de origen
    const { rows: impactos } = await cliente.query(`
      SELECT 
        usuario_raw, 
        grupo_raw, 
        nombre_push, 
        caption, 
        timestamp_msg
      FROM impactos_raw
      WHERE hash_largo = $1
      ORDER BY id ASC
      LIMIT 2
    `, [hash_largo]);

    if (!impactos || impactos.length === 0) {
      throw new Error(`No se encontraron registros en impactos_raw para el hash ${hash_largo}`);
    }

    // 2. Obtener datos asentados en comprobantes_raw
    const { rows: comprobantes } = await cliente.query(`
      SELECT monto, moneda, banco, referencia, titular, url_r2
      FROM comprobantes_raw
      WHERE hash_largo = $1
      LIMIT 1
    `, [hash_largo]);

    const datosIA = comprobantes[0] || {};
    const impacto1 = impactos[0];
    const impacto2 = impactos[1] || {};

    // 3. Construir Payload Maestro
    const payloadMaestro = {
      evento: "comprobante.validado",
      identificadores: {
        hash_largo,
        hash_corto: hashCorto,
        instancia: instancia || 'JAIRO'
      },
      extraccion_ia: {
        monto: datosIA.monto ?? null,
        moneda: datosIA.moneda || null,
        banco: datosIA.banco || null,
        referencia: datosIA.referencia || null,
        titular: datosIA.titular || null
      },
      imagen_url: resolverUrlImagen(datosIA.url_r2),
      origen_impacto_1: {
        usuario_jid: impacto1.usuario_raw || null,
        grupo_jid: impacto1.grupo_raw || null,
        nombre_push: impacto1.nombre_push || null,
        caption: impacto1.caption || null,
        timestamp: impacto1.timestamp_msg ? Number(impacto1.timestamp_msg) : null
      },
      origen_impacto_2: impactos.length > 1 ? {
        usuario_jid: impacto2.usuario_raw || null,
        grupo_jid: impacto2.grupo_raw || null,
        nombre_push: impacto2.nombre_push || null,
        caption: impacto2.caption || null,
        timestamp: impacto2.timestamp_msg ? Number(impacto2.timestamp_msg) : null
      } : null,
      timestamp_despacho: new Date().toISOString()
    };

    // 4. Enviar Webhook
    await enviarWebhookFinal(payloadMaestro);

    console.log(`[Distribuidor] 🚀 Comprobante ${hashCorto} entregado exitosamente.`);
    return true;

  } catch (error) {
    console.error(`[Distribuidor ERROR] Falló el procesamiento del Hash ${hashCorto}:`, error.message);
    throw error;
  } finally {
    cliente.release();
  }
}

module.exports = { procesarDistribucion, resolverUrlImagen };
