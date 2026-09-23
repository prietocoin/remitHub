function resolverUrlImagen(rawPathOrUrl, fallbackHashKey) {
  const r2Domain = (process.env.R2_PUBLIC_DOMAIN || '').replace(/\/$/, '');

  if (rawPathOrUrl) {
    const keyMatch = rawPathOrUrl.match(/comprobantes\/[^\s"']+/);
    if (keyMatch && r2Domain) {
      return `${r2Domain}/${keyMatch[0]}`;
    }
    if (rawPathOrUrl.startsWith('http') && !rawPathOrUrl.includes('pub-xxxx') && !rawPathOrUrl.includes('automat-panel')) {
      return rawPathOrUrl;
    }
    if (r2Domain) {
      const cleanKey = rawPathOrUrl.replace(/^https?:\/\/[^\/]+\//, '');
      return `${r2Domain}/${cleanKey}`;
    }
  }

  if (r2Domain && fallbackHashKey) {
    return `${r2Domain}/comprobantes/${fallbackHashKey}.jpg`;
  }

  return rawPathOrUrl || null;
}

module.exports = { resolverUrlImagen };
