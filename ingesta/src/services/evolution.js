const axios = require('axios');

async function obtenerBufferImagen(instancia, instanceId, key, message) {
  const baseUrl = process.env.EVOLUTION_API_URL;
  const apiKey = process.env.AUTHENTICATION_API_KEY || process.env.EVOLUTION_API_KEY;

  if (!baseUrl) {
    console.error('[Evolution Service] ❌ EVOLUTION_API_URL no configurada');
    return null;
  }

  const cleanBaseUrl = baseUrl.replace(/\/$/, '');
  
  // Requerido: key + message (contiene mediaKey y directPath para desencriptar al vuelo)
  const payload = {
    message: {
      key: key,
      message: message
    },
    convertToMp4: false
  };

  const headers = {
    'apikey': apiKey,
    'Content-Type': 'application/json'
  };

  let target = instancia || instanceId;
  let url = `${cleanBaseUrl}/message/getBase64FromMediaMessage/${target}`;

  try {
    const response = await axios.post(url, payload, { headers, timeout: 15000 });
    const base64Data = response.data?.base64 || response.data?.media;
    
    if (typeof base64Data === 'string') {
      const cleanBase64 = base64Data.replace(/^data:image\/\w+;base64,/, '');
      return Buffer.from(cleanBase64, 'base64');
    }
  } catch (err) {
    if (err.response?.status === 404 && instanceId && target !== instanceId) {
      console.warn(`[Evolution Service ⚠️] 404 con "${target}". Reintentando con UUID "${instanceId}"...`);
      try {
        const fallbackUrl = `${cleanBaseUrl}/message/getBase64FromMediaMessage/${instanceId}`;
        const responseFallback = await axios.post(fallbackUrl, payload, { headers, timeout: 15000 });
        const base64Data = responseFallback.data?.base64 || responseFallback.data?.media;
        
        if (typeof base64Data === 'string') {
          const cleanBase64 = base64Data.replace(/^data:image\/\w+;base64,/, '');
          return Buffer.from(cleanBase64, 'base64');
        }
      } catch (fallbackErr) {
        console.error(`[Evolution Service ERROR] Fallaron ambos identificadores multitenant (${instancia} / ${instanceId}):`, fallbackErr.message);
      }
    } else {
      console.error(`[Evolution Service ERROR] HTTP ${err.response?.status || '500'} (${target}):`, err.response?.data?.message || err.message);
    }
  }

  return null;
}

module.exports = { obtenerBufferImagen };
