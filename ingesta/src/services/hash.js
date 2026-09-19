const crypto = require('crypto');

/**
 * Genera una huella inmutable SHA-256 de 64 caracteres
 */
function generarSha256(buffer, fallbackIdentifier) {
  if (Buffer.isBuffer(buffer) && buffer.length > 0) {
    return crypto.createHash('sha256').update(buffer).digest('hex').toUpperCase();
  }
  
  const rawId = fallbackIdentifier || `msg_${Date.now()}_${Math.random()}`;
  return crypto.createHash('sha256').update(String(rawId)).digest('hex').toUpperCase();
}

module.exports = { generarSha256 };
