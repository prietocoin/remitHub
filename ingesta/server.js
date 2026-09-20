const express = require('express');
const pool = require('./src/config/db');
const { normalizarHashWhatsApp } = require('./src/services/hash');
const validadorQueue = require('./src/queues/validador.queue');

const app = express();
app.use(express.json({ limit: '50mb' }));

app.get('/', (req, res) => res.status(200).json({ status: 'ok', service: 'ingesta-api' }));

app.post('/api/v1/webhook/whatsapp', async (req, res) => {
  // 1. Responder de inmediato a Evolution API / n8n
  res.status(200).json({ status: 'received', timestamp: Date.now() });

  try {
    // Normalizar si la petición viene envuelta en un Array (caso común en n8n)
    const rawBody = Array.isArray(req.body) ? req.body[0] : req.body;
    const body = rawBody?.body || rawBody || {};
    const data = body?.data || {};
    const key = data?.key || {};
    const message = data?.message || {};
    const imageMsg = message?.imageMessage;

    // Si no es un mensaje con imagen, se omite de este flujo
    if (!imageMsg && data?.messageType !== 'imageMessage') {
      return;
    }

    // 2. Extraer Hash SHA-256 nativo enviando el objeto de bytes
    const hashLargo = normalizarHashWhatsApp(imageMsg?.fileSha256, key?.id);
    const hashCorto = hashLargo ? hashLargo.slice(-8) : null;

    // 3. Extraer metadatos del mensaje
    const instancia = body?.instance || data?.instance || 'DEFAULT';
    const usuarioRaw = key?.participantAlt || key?.participant || key?.remoteJid || null;
    const grupoRaw = key?.participant ? key?.remoteJid : null;
    const nombrePush = data?.pushName || body?.nombre_push || 'Desconocido';
    const caption = imageMsg?.caption || message?.conversation || '';
    const timestampMsg = Number(data?.messageTimestamp || Math.floor(Date.now() / 1000));

    // 4. Inserción Ciega inmutable en PostgreSQL (impactos_raw)
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

    console.log(`[Express Ingesta] 📥 Impacto #${impactoId} registrado en DB (Hash: ${hashCorto})`);

    // 5. Encolar para el Validador
    await validadorQueue.add('validar-evento', {
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
    console.error('[Express Ingesta ERROR] Fallo procesando webhook:', err.message);
  }
});

module.exports = app;
