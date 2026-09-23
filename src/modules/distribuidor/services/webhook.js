const axios = require('axios');

async function enviarWebhookFinal(payloadMaestro) {
  const webhookUrl = process.env.DISTRIBUIDOR_WEBHOOK_URL;

  if (!webhookUrl) {
    console.warn('[Distribuidor ⚠️] DISTRIBUIDOR_WEBHOOK_URL no configurada. Imprimiendo payload localmente:');
    console.log(JSON.stringify(payloadMaestro, null, 2));
    return true;
  }

  try {
    const response = await axios.post(webhookUrl, payloadMaestro, {
      headers: { 'Content-Type': 'application/json' },
      timeout: 15000,
    });

    console.log(`[Distribuidor HTTP] ✅ Payload enviado con éxito (Status HTTP: ${response.status})`);
    return true;
  } catch (error) {
    console.error(`[Distribuidor HTTP ❌] Falló el envío del Webhook:`, error.response?.data || error.message);
    throw error;
  }
}

module.exports = { enviarWebhookFinal };
