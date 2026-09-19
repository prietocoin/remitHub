const axios = require('axios');

async function obtenerBufferImagen(instancia, key, message) {
  const baseUrl = process.env.EVOLUTION_API_URL;
  const apiKey = process.env.AUTHENTICATION_API_KEY || process.env.EVOLUTION_API_KEY;

  if (!baseUrl) {
    console.error('[Evolution Service] ❌ EVOLUTION_API_URL no configurada');
    return null;
  }

  try {
    const cleanBaseUrl = baseUrl.replace(/\/$/, '');
    const url = `${cleanBaseUrl}/message/getBase64FromMediaMessage/${instancia}`;

    const response = await axios.post(url, {
      message: {
        key: key,
        message: message
      }
    }, {
      headers: {
        'apikey': apiKey,
        'Content-Type': 'application/json'
      },
      timeout: 15000
    });

    const base64Data = response.data?.base64 || response.data?.media;
    if (typeof base64Data === 'string') {
      const cleanBase64 = base64Data.replace(/^data:image\/\w+;base64,/, '');
      return Buffer.from(cleanBase64, 'base64');
    } else {
      console.error('[Evolution Service ⚠️] Respuesta sin Base64:', JSON.stringify(response.data));
    }
  } catch (err) {
    console.error(`[Evolution Service ERROR] HTTP ${err.response?.status || '500'} (${instancia}):`, err.message);
  }

  return null;
}

module.exports = { obtenerBufferImagen };
