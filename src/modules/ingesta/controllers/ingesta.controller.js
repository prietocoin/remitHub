const pool = require('../../../config/db');
const { normalizarHashWhatsApp } = require('../services/hash');

async function procesarWebhookIngesta(req, res, pipelineQueue) {
  // 1. Responder 200 de inmediato
  res.status(200).json({ status: 'received', timestamp: Date.now() });

  try {
    const rawBody = Array.isArray(req.body) ? req.body[0] : req.body;
    const body = rawBody?.body || rawBody || {};
    const data = body?.data || {};
    const key = data?.key || {};
    const message = data?.message || {};
    const imageMsg = message?.imageMessage;

    if (!imageMsg && data?.messageType !== 'imageMessage') {
      return;
    }

    // 2. Normalizar Hash
    const hashLargo = normalizarHashWhatsApp(imageMsg?.fileSha256, key?.id);
    const hashCorto = hashLargo ? hashLargo.slice(-8) : null;

    // 3. Metadatos
    const instancia = body?.instance || data?.instance || 'DEFAULT';
    const usuarioRaw = key?.participantAlt || key?.participant || key?.remoteJid || null;
    const grupoRaw = key?.participant ? key?.remoteJid : null;
    const nombrePush = data?.pushName || body?.nombre_push || 'Desconocido';
    const caption = imageMsg?.caption || message?.conversation || '';
    const timestampMsg = Number(data?.messageTimestamp || Math.floor(Date.now() / 1000));

    // 4. Inserción inmutable en DB
    const query = `
      INSERT INTO impactos_raw 
        (hash_largo, hash_corto, instancia, usuario_raw, grupo_raw, nombre_push, caption, timestamp_msg)
      VALUES 
        ($1, $2, $3, $4, $5, $6, $7, $8)
      RETURNING id;
    `;
    const values = [hashLargo, hashCorto, instancia, usuarioRaw, grupoRaw, nombrePush, caption, timestampMsg];
    const { rows } = await pool.query(query, values);
    const impactoId = rows[0].id;

    console.log(`[remitHub Ingesta] 📥 Impacto #${impactoId} registrado en DB (Hash: ${hashCorto})`);

    // 5. Encolar en el pipeline unificado de remitHub
    await pipelineQueue.add('procesar-comprobante', {
      impactoId,
      hashLargo,
      hashCorto,
      instancia,
      usuarioRaw,
      grupoRaw,
      nombrePush,
      caption,
      timestampMsg,
      rawPayload: req.body
    });

  } catch (err) {
    console.error('[remitHub Ingesta ERROR] Fallo procesando webhook:', err.message);
  }
}

module.exports = { procesarWebhookIngesta };
