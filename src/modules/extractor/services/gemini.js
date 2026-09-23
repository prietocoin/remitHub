const axios = require('axios');

const rawKeys = process.env.GEMINI_KEYS || process.env.GEMINI_API_KEY || '';
const geminiKeyList = rawKeys.split(',').map(k => k.trim().replace(/^["']|["']$/g, '')).filter(Boolean);
let currentKeyIndex = 0;

function getNextActiveKey() {
  if (geminiKeyList.length === 0) return null;
  const key = geminiKeyList[currentKeyIndex % geminiKeyList.length];
  currentKeyIndex = (currentKeyIndex + 1) % geminiKeyList.length;
  return key;
}

/**
 * Extrae y parsea limpiamente la estructura JSON enviada por el modelo,
 * ignorando texto secundario o bloques de código markdown.
 */
function parsearJSONSeguro(texto) {
  if (!texto) throw new Error('Gemini devolvió una respuesta vacía.');

  const match = texto.match(/\{[\s\S]*\}/);
  if (!match) {
    throw new Error(`No se encontró un JSON válido en la respuesta: "${texto.substring(0, 100)}..."`);
  }

  const jsonString = match[0].trim();
  
  try {
    return JSON.parse(jsonString);
  } catch (e) {
    throw new Error(`Sintaxis JSON inválida devuelta por Gemini: ${e.message}`);
  }
}

async function extraerDatosConGemini(prompt, mimeType, imageBase64) {
  if (geminiKeyList.length === 0) {
    throw new Error('No hay llaves de API de Gemini configuradas.');
  }

  // Se toma estrictamente el modelo definido en tu .env (remueve el prefijo "models/" si existiera)
  const rawModel = process.env.GEMINI_MODEL || 'gemini-3.5-flash-lite';
  const cleanModel = rawModel.replace(/^models\//, '').trim();

  let ultimoError = null;
  const maxIntentosKeys = Math.min(geminiKeyList.length, 3);

  // Reintenta rotando las llaves de API si alguna da error
  for (let intentoKey = 0; intentoKey < maxIntentosKeys; intentoKey++) {
    const activeKey = getNextActiveKey();
    const url = `https://generativelanguage.googleapis.com/v1beta/models/${cleanModel}:generateContent?key=${activeKey}`;

    try {
      const response = await axios.post(url, {
        contents: [{
          parts: [
            { text: prompt },
            { inline_data: { mime_type: mimeType, data: imageBase64 } }
          ]
        }],
        generationConfig: { 
          responseMimeType: "application/json",
          temperature: 0.1 
        }
      }, { timeout: 35000 });

      const textResult = response.data?.candidates?.[0]?.content?.parts?.[0]?.text;
      
      return parsearJSONSeguro(textResult);

    } catch (err) {
      ultimoError = err;
      const status = err.response?.status;
      console.warn(`[Gemini Service ⚠️] Petición con el modelo "${cleanModel}" falló (HTTP ${status || 'Err'}): ${err.message}`);
    }
  }

  throw new Error(`Gemini falló tras reintentar con las claves de la API: ${ultimoError?.message}`);
}

module.exports = { extraerDatosConGemini };
