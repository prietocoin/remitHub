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

  // Modelo preferido del .env
  const rawModel = (process.env.GEMINI_MODEL || 'gemini-3.8-flash-lite').replace(/^models\//, '').trim();

  // Lista en cascada para rotar modelos automáticamente en caso de error HTTP 503/429
  const listaModelosPila = [
    rawModel,
    'gemini-3.8-flash-lite',
    'gemini-3.8-flash',
    'gemini-3.7-flash-lite',
    'gemini-3.6-flash-lite',
    'gemini-3.5-flash-lite',
    'gemini-1.5-flash'
  ];

  // Filtrar duplicados manteniendo orden de prioridad
  const modelosUnicos = [...new Set(listaModelosPila)];

  let ultimoError = null;

  // Itera por la cascada de modelos (3.8 -> 3.7 -> 3.6 -> 3.5)
  for (const model of modelosUnicos) {
    const maxIntentosKeys = Math.min(geminiKeyList.length, 3);

    // Itera rotando las llaves de API configuradas
    for (let intentoKey = 0; intentoKey < maxIntentosKeys; intentoKey++) {
      const activeKey = getNextActiveKey();
      const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${activeKey}`;

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
        console.warn(`[Gemini Service ⚠️] Modelo "${model}" con Key #${intentoKey + 1} falló (HTTP ${status || 'Err'}): ${err.message}`);
      }
    }
  }

  throw new Error(`Gemini falló tras probar todos los modelos de respaldo (3.8, 3.7, 3.6, 3.5) y claves: ${ultimoError?.message}`);
}

module.exports = { extraerDatosConGemini };
