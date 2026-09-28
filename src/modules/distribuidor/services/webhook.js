// src/modules/distribuidor/services/webhook.js
const axios = require('axios');

async function enviarWebhookFinal(payloadMaestro) {
  const webhookUrl = process.env.DISTRIBUIDOR_WEBHOOK_URL;

  if (!webhookUrl) {
    return true;
  }

  try {
    const response = await axios.post(webhookUrl, payloadMaestro, {
      headers: { 'Content-Type': 'application/json' },
      timeout: 15000,
    });
    return true;
  } catch (error) {
    // 🟢 Evita que un error 404 en el webhook cancele el trabajo en BullMQ
    console.warn(`[Distribuidor HTTP ⚠️] Webhook omitido (${error.message}). La lectura de IA ya fue guardada en BD.`);
    return true;
  }
}

module.exports = { enviarWebhookFinal };
