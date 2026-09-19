const axios = require('axios');

async function obtenerBufferImagen(instancia, key, message) {
  const baseUrl = process.env.EVOLUTION_API_URL;
  const apiKey = process.env.AUTHENTICATION_API_KEY || process.env.EVOLUTION_API_KEY;

  if (!baseUrl) {
    console.error('[Evolution Service] ❌ EVOLUTION_API_URL no configurada');
    return null;
  }

  try {
    const url = `${baseUrl.replace(/\/$/, '')}/message/getBase64FromMediaMessage/${instancia}`;
    const response = await axios.post(url, {
      message: { key, message }
    }, {
      headers: { 'apikey': apiKey, 'Content-Type': 'application/json' },
      timeout: 12000
    });

    const base64Data = response.data?.base64 || response.data?.media;
    if (typeof base64Data === 'string') {
      const cleanBase64 = base64Data.replace(/^data:image\/\w+;base64,/, '');
      return Buffer.from(cleanBase64, 'base64');
    }
  } catch (err) {
    console.error(`[Evolution Service ERROR] Falló descarga de media (${instancia}):`, err.message);
    throw err; // Re-lanzar para que BullMQ active el reintento automático
  }

  return null;
}

module.exports = { obtenerBufferImagen };
