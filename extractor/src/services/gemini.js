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

function parsearJSONSeguro(texto) {
  if (!texto) return {};
  const limpio = texto.replace(/^```json/gi, '').replace(/```$/g, '').trim();
  try {
    return JSON.parse(limpio);
  } catch (e) {
    console.warn('[Gemini Service ⚠️] No se pudo parsear JSON, devolviendo objeto vacío.');
    return {};
  }
}

async function extraerDatosConGemini(prompt, mimeType, imageBase64) {
  if (geminiKeyList.length === 0) {
    throw new Error('No hay llaves de API de Gemini configuradas.');
  }

  const modelosCandidatos = [
    process.env.GEMINI_MODEL,
    'gemini-3.5-flash',
    'gemini-2.5-flash',
    'gemini-2.0-flash',
    'gemini-1.5-flash'
  ].filter(Boolean);

  let ultimoError = null;
  const maxIntentosKeys = Math.min(geminiKeyList.length, 3);

  for (let intentoKey = 0; intentoKey < maxIntentosKeys; intentoKey++) {
    const activeKey = getNextActiveKey();

    for (const rawModel of modelosCandidatos) {
      const cleanModel = rawModel.replace(/^models\//, '');
      const url = `https://generativelanguage.googleapis.com/v1beta/models/${cleanModel}:generateContent?key=${activeKey}`;

      try {
        const response = await axios.post(url, {
          contents: [{
            parts: [
              { text: prompt },
              { inline_data: { mime_type: mimeType, data: imageBase64 } }
            ]
          }],
          generationConfig: { responseMimeType: "application/json" }
        }, { timeout: 35000 });

        const textResult = response.data?.candidates?.[0]?.content?.parts?.[0]?.text;
        return parsearJSONSeguro(textResult);

      } catch (err) {
        ultimoError = err;
        const status = err.response?.status;
        console.warn(`[Gemini Service ⚠️] Modelo "${cleanModel}" falló (HTTP ${status || 'Err'}): ${err.message}`);

        if (status === 404) continue;
        break;
      }
    }
  }

  throw new Error(`Gemini falló en todos los reintentos: ${ultimoError?.message}`);
}

module.exports = { extraerDatosConGemini };
