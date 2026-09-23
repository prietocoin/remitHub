const axios = require('axios');

/**
 * Obtiene el Buffer binario de la imagen desde Evolution API (Soporta v1 y v2)
 */
async function obtenerBufferImagen(instancia, instanceId, key, message) {
  const baseUrl = process.env.EVOLUTION_API_URL;
  const apiKey = process.env.AUTHENTICATION_API_KEY || process.env.EVOLUTION_API_KEY;

  if (!baseUrl) {
    console.error('[Evolution Service ❌] EVOLUTION_API_URL no está configurada.');
    return null;
  }

  const cleanBaseUrl = baseUrl.replace(/\/$/, '');
  const target = instancia || instanceId;

  if (!target) {
    console.error('[Evolution Service ⚠️] Petición cancelada: No se proporcionó instancia ni instanceId.');
    return null;
  }

  // Candidatos de endpoints para compatibilidad con distintas versiones de Evolution API
  const endpoints = [
    `${cleanBaseUrl}/message/getBase64FromMediaMessage/${target}`,
    `${cleanBaseUrl}/chat/getBase64FromMediaMessage/${target}`,
  ];

  if (instanceId && target !== instanceId) {
    endpoints.push(`${cleanBaseUrl}/message/getBase64FromMediaMessage/${instanceId}`);
    endpoints.push(`${cleanBaseUrl}/chat/getBase64FromMediaMessage/${instanceId}`);
  }

  const payload = {
    message: { key, message },
    convertToMp4: false
  };

  const headers = {
    'apikey': apiKey,
    'Content-Type': 'application/json'
  };

  for (const url of endpoints) {
    try {
      const response = await axios.post(url, payload, { headers, timeout: 12000 });
      const base64Data = response.data?.base64 || response.data?.media;

      if (typeof base64Data === 'string') {
        const cleanBase64 = base64Data.replace(/^data:image\/\w+;base64,/, '');
        return Buffer.from(cleanBase64, 'base64');
      }
    } catch (err) {
      if (err.response?.status === 404) continue;
      console.error(`[Evolution Service ERROR] HTTP ${err.response?.status || '500'} (${url}):`, err.response?.data?.message || err.message);
      break;
    }
  }

  console.error(`[Evolution Service ⚠️] No se pudo descargar la imagen para "${target}".`);
  return null;
}

module.exports = { obtenerBufferImagen };
