const crypto = require('crypto');

/**
 * Convierte el objeto de bytes fileSha256 que envía WhatsApp/Evolution API
 * a una huella Hexadecimal SHA-256 de 64 caracteres en mayúsculas.
 */
function normalizarHashWhatsApp(fileSha256, fallbackId) {
  if (fileSha256 && typeof fileSha256 === 'object') {
    const byteArray = Object.values(fileSha256);
    if (byteArray.length === 32) {
      return byteArray
        .map(byte => byte.toString(16).padStart(2, '0'))
        .join('')
        .toUpperCase();
    }
  }

  // Generación de respaldo si la imagen no incluye fileSha256
  const rawId = fallbackId || `msg_${Date.now()}_${Math.random()}`;
  return crypto.createHash('sha256').update(String(rawId)).digest('hex').toUpperCase();
}

module.exports = { normalizarHashWhatsApp };
