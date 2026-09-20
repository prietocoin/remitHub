const express = require('express');
const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT) || 5432,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

const app = express();
app.use(express.json());
const PORT = process.env.PORT || 3000;

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

// 1. ENDPOINTS API

app.get('/api/instancias', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT DISTINCT LOWER(instancia) as instancia 
      FROM registros_raw 
      WHERE instancia IS NOT NULL AND instancia <> ''
      ORDER BY instancia ASC
    `);
    res.json(rows.map(r => r.instancia));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/comprobantes', async (req, res) => {
  try {
    const instanciaTarget = req.query.instancia || 'JAIRO';
    const soloBinomios = req.query.solo_binomios === 'true';

    let filtroConteo = '';
    if (soloBinomios) {
      filtroConteo = 'WHERE GREATEST(r.total_impactos, r.conteo_max) > 1';
    }

    const query = `
      WITH ranked_raw AS (
        SELECT 
          *,
          ROW_NUMBER() OVER (PARTITION BY hash_largo ORDER BY timestamp_msg ASC, ctid ASC) as num_impacto
        FROM registros_raw
        WHERE LOWER(instancia) = LOWER($1)
      ),
      raw_consolidado AS (
        SELECT 
          hash_largo,
          MAX(instancia) as instancia,
          COUNT(*) as total_impactos,
          MAX(conteo) as conteo_max,
          MAX(timestamp_msg) as timestamp_msg,
          -- Impacto 1 (Primer mensaje recibido)
          MAX(NULLIF(nombre_push, '')) FILTER (WHERE num_impacto = 1) as nombre_push_1,
          MAX(NULLIF(usuario_raw, '')) FILTER (WHERE num_impacto = 1) as usuario_raw_1,
          MAX(NULLIF(grupo_raw, '')) FILTER (WHERE num_impacto = 1) as grupo_raw_1,
          MAX(NULLIF(caption, '')) FILTER (WHERE num_impacto = 1) as caption_1,
          MAX(NULLIF(url_imagen, '')) FILTER (WHERE num_impacto = 1) as url_imagen_1,
          -- Impacto 2 (Segundo mensaje recibido)
          MAX(NULLIF(nombre_push, '')) FILTER (WHERE num_impacto = 2) as nombre_push_2,
          MAX(NULLIF(usuario_raw, '')) FILTER (WHERE num_impacto = 2) as usuario_raw_2,
          MAX(NULLIF(grupo_raw, '')) FILTER (WHERE num_impacto = 2) as grupo_raw_2,
          MAX(NULLIF(caption, '')) FILTER (WHERE num_impacto = 2) as caption_2,
          MAX(NULLIF(url_imagen, '')) FILTER (WHERE num_impacto = 2) as url_imagen_2,
          MAX(estado) as estado_raw
        FROM ranked_raw
        GROUP BY hash_largo
      )
      SELECT 
        r.hash_largo,
        COALESCE(c.estado_ia, CASE WHEN GREATEST(r.total_impactos, r.conteo_max) >= 2 THEN 'PROCESADO' ELSE r.estado_raw END) as estado,
        r.timestamp_msg,
        r.instancia,
        GREATEST(r.total_impactos, r.conteo_max) as conteo,
        r.nombre_push_1,
        r.usuario_raw_1,
        r.grupo_raw_1,
        r.caption_1,
        r.url_imagen_1,
        r.nombre_push_2,
        r.usuario_raw_2,
        r.grupo_raw_2,
        r.caption_2,
        r.url_imagen_2,
        c.url_r2 as url_r2_comprobante,
        c.monto,
        c.moneda,
        c.banco,
        c.referencia,
        c.titular,
        c.estado_ia
      FROM raw_consolidado r
      LEFT JOIN comprobantes_raw c ON r.hash_largo = c.hash_largo
      ${filtroConteo}
      ORDER BY r.timestamp_msg DESC
      LIMIT 60
    `;
    const { rows } = await pool.query(query, [instanciaTarget]);

    const itemsFormateados = rows.map(row => ({
      ...row,
      url_imagen_1: resolverUrlImagen(row.url_imagen_1 || row.url_r2_comprobante, row.hash_largo),
      url_imagen_2: row.url_imagen_2 ? resolverUrlImagen(row.url_imagen_2, row.hash_largo + '_2') : null
    }));

    res.json(itemsFormateados);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/comprobantes/:hash', async (req, res) => {
  const { hash } = req.params;
  try {
    await pool.query(`UPDATE registros_raw SET estado = 'DESCARTADO' WHERE hash_largo = $1`, [hash]);
    res.json({ success: true, hash });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 2. DASHBOARD WEB
app.get('/', (req, res) => {
  res.send(`
<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Observabilidad SaaS - Panel</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style> body { background-color: #0d131f; } .card-bg { background-color: #161f30; } </style>
</head>
<body class="text-slate-200 min-h-screen p-4 md:p-6 font-sans">
  <div class="max-w-7xl mx-auto space-y-6">
    
    <div class="flex flex-col md:flex-row justify-between items-start md:items-center gap-4 border-b border-slate-800 pb-4">
      <div class="flex items-center gap-3">
        <span class="text-3xl">👁️</span>
        <div>
          <h1 class="text-xl font-bold text-white tracking-wide">Monitor de Ecosistema</h1>
          <p class="text-xs text-slate-400">Auditoría cruzada: RAW + IA</p>
        </div>
      </div>

      <div class="flex flex-wrap items-center gap-3">
        <div class="flex items-center gap-2 bg-slate-900 border border-slate-700 rounded-lg px-3 py-1.5">
          <label for="select-filtro" class="text-xs font-semibold text-slate-400">Mostrar:</label>
          <select id="select-filtro" onchange="cambiarFiltro(this.value)" class="bg-transparent text-emerald-400 font-bold text-xs focus:outline-none cursor-pointer">
            <option value="todos" class="bg-slate-900 text-white">Todos los Eventos</option>
            <option value="binomios" class="bg-slate-900 text-white">Solo Comprobantes (>1x)</option>
          </select>
        </div>

        <div class="flex items-center gap-2 bg-slate-900 border border-slate-700 rounded-lg px-3 py-1.5">
          <label for="select-instancia" class="text-xs font-semibold text-slate-400">Instancia:</label>
          <select id="select-instancia" onchange="cambiarInstancia(this.value)" class="bg-transparent text-sky-400 font-bold text-sm focus:outline-none cursor-pointer">
            <option value="JAIRO" class="bg-slate-900 text-white">JAIRO</option>
          </select>
        </div>

        <span class="bg-slate-800/80 px-3 py-1.5 rounded-lg text-xs font-semibold text-slate-300 border border-slate-700">
          Registros Visibles: <strong id="c-total" class="text-white">0</strong>
        </span>
      </div>
    </div>

    <div id="grid-container" class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
      <div class="col-span-full text-center py-12 text-slate-500">Cargando datos...</div>
    </div>
  </div>

  <script>
    const urlParams = new URLSearchParams(window.location.search);
    let INSTANCIA_ACTUAL = urlParams.get('instancia') || 'JAIRO';
    let SOLO_BINOMIOS = urlParams.get('filtro') === 'binomios';

    document.getElementById('select-filtro').value = SOLO_BINOMIOS ? 'binomios' : 'todos';

    async function cargarInstancias() {
      try {
        const res = await fetch('/api/instancias');
        const lista = await res.json();
        
        if (Array.isArray(lista) && lista.length > 0) {
          const select = document.getElementById('select-instancia');
          const setInstancias = new Set([...lista, INSTANCIA_ACTUAL.toLowerCase()]);
          
          select.innerHTML = Array.from(setInstancias).map(inst => {
            const nombre = inst.toUpperCase();
            const selected = inst.toLowerCase() === INSTANCIA_ACTUAL.toLowerCase() ? 'selected' : '';
            return \`<option value="\${nombre}" \${selected} class="bg-slate-900 text-white">\${nombre}</option>\`;
          }).join('');
        }
      } catch (e) { console.error('Error cargando instancias:', e); }
    }

    function cambiarInstancia(nuevaInstancia) {
      INSTANCIA_ACTUAL = nuevaInstancia;
      actualizarURL();
      cargar();
    }

    function cambiarFiltro(val) {
      SOLO_BINOMIOS = (val === 'binomios');
      actualizarURL();
      cargar();
    }

    function actualizarURL() {
      const p = new URLSearchParams();
      p.set('instancia', INSTANCIA_ACTUAL);
      if (SOLO_BINOMIOS) p.set('filtro', 'binomios');
      window.history.pushState({}, '', '?' + p.toString());
    }

    async function borrarRegistro(hash) {
      if(!confirm('¿Deseas descartar este registro (Soft-Delete)?')) return;
      try {
        await fetch('/api/comprobantes/' + hash, { method: 'DELETE' });
        cargar();
      } catch(e) { console.error(e); }
    }

    async function cargar() {
      try {
        const url = \`/api/comprobantes?instancia=\${encodeURIComponent(INSTANCIA_ACTUAL)}&solo_binomios=\${SOLO_BINOMIOS}\`;
        const res = await fetch(url);
        const items = await res.json();

        if (!Array.isArray(items)) {
          document.getElementById('grid-container').innerHTML = \`<div class="col-span-full text-center py-12 text-rose-400 font-mono text-xs">⚠️ Error: \${items.error || 'Desconocido'}</div>\`;
          return;
        }

        document.getElementById('c-total').innerText = items.length;

        if (items.length === 0) {
          document.getElementById('grid-container').innerHTML = \`<div class="col-span-full text-center py-12 text-slate-500">No hay registros para \${INSTANCIA_ACTUAL.toUpperCase()}.</div>\`;
          return;
        }

        const html = items.map(item => {
          const estado = item.estado || 'RECIBIDO';
          const totalConteo = item.conteo || 1;
          
          let badgeColor = 'bg-slate-500/10 text-slate-400 border-slate-500/30';
          if (estado === 'PROCESADO') badgeColor = 'bg-emerald-500/10 text-emerald-400 border-emerald-500/30';
          if (estado === 'DESCARTADO' || estado === 'CADUCADO') badgeColor = 'bg-amber-500/10 text-amber-400 border-amber-500/30';
          if (estado === 'FALLO') badgeColor = 'bg-rose-500/10 text-rose-400 border-rose-500/30';

          const badgeHTML = \`<span class="\${badgeColor} border text-[10px] font-bold px-2 py-0.5 rounded-full">\${estado}</span>\`;
          const conteoHTML = \`<span class="bg-indigo-500/20 text-indigo-300 border border-indigo-500/40 text-[10px] font-black px-2 py-0.5 rounded-full">\${totalConteo}x</span>\`;

          let fechaTexto = 'Sin Fecha';
          if (item.timestamp_msg) {
            const ts = Number(item.timestamp_msg);
            fechaTexto = new Date(ts > 9999999999 ? ts : ts * 1000).toLocaleString('es-ES');
          }

          // Bloque Lectura IA incluyendo Titular
          let extraccionIA = '';
          if (item.monto || item.banco || item.referencia || item.titular) {
            extraccionIA = \`
              <div class="mt-2 p-2 bg-emerald-950/40 border border-emerald-700/50 rounded-lg flex flex-col gap-1">
                <span class="text-[9px] text-emerald-400 font-extrabold uppercase tracking-wider">LECTURA IA</span>
                <span class="text-[11px] text-emerald-300 font-bold">\${item.monto || '0'} \${item.moneda || ''} - \${item.banco || 'N/A'}</span>
                <div class="text-[10px] text-emerald-200/90 font-medium truncate" title="\${item.titular || 'N/A'}">
                  👤 Titular: <strong>\${item.titular || 'N/A'}</strong>
                </div>
                <span class="text-[10px] text-emerald-400/80 font-mono">Ref: \${item.referencia || 'N/A'}</span>
              </div>
            \`;
          }

          // Renderizado de Imágenes Duales (Img 1 e Img 2)
          let img1HTML = item.url_imagen_1 
            ? \`<img src="\${item.url_imagen_1}" class="w-full h-full object-cover cursor-pointer hover:scale-105 transition" onclick="window.open(this.src)" title="Imagen 1 - Click para expandir" onerror="this.onerror=null; this.parentElement.innerHTML='<div class=\\'w-full h-full flex items-center justify-center text-[9px] text-amber-400 font-mono text-center px-1\\'>Img 1 R2</div>';"/>\`
            : '<div class="w-full h-full flex items-center justify-center text-[9px] text-slate-600 font-mono text-center">Sin Img 1</div>';

          let img2HTML = item.url_imagen_2 
            ? \`<img src="\${item.url_imagen_2}" class="w-full h-full object-cover cursor-pointer hover:scale-105 transition" onclick="window.open(this.src)" title="Imagen 2 - Click para expandir" onerror="this.onerror=null; this.parentElement.innerHTML='<div class=\\'w-full h-full flex items-center justify-center text-[9px] text-amber-400 font-mono text-center px-1\\'>Img 2 R2</div>';"/>\`
            : null;

          let containerImagenes = '';
          if (img2HTML) {
            containerImagenes = \`
              <div class="flex flex-col gap-1.5 w-28 flex-shrink-0">
                <div class="h-24 bg-slate-900 rounded-lg overflow-hidden border border-slate-800 relative">
                  <span class="absolute top-0.5 left-0.5 bg-slate-950/80 text-[8px] text-slate-300 px-1 rounded z-10">Img 1</span>
                  \${img1HTML}
                </div>
                <div class="h-24 bg-slate-900 rounded-lg overflow-hidden border border-slate-800 relative">
                  <span class="absolute top-0.5 left-0.5 bg-indigo-950/80 text-[8px] text-indigo-300 px-1 rounded z-10">Img 2</span>
                  \${img2HTML}
                </div>
              </div>
            \`;
          } else {
            containerImagenes = \`
              <div class="w-28 h-48 bg-slate-900 rounded-lg overflow-hidden border border-slate-800 flex-shrink-0">
                \${img1HTML}
              </div>
            \`;
          }

          // Datos Impacto 1
          const p1 = item.nombre_push_1 || 'Desconocido';
          const u1 = item.usuario_raw_1 ? \`<span class="text-[9px] text-slate-400 font-mono block truncate" title="\${item.usuario_raw_1}">JID 1: \${item.usuario_raw_1}</span>\` : '';
          const g1 = item.grupo_raw_1 ? \`<span class="text-[9px] text-indigo-400/80 font-mono block truncate" title="\${item.grupo_raw_1}">Grupo 1: \${item.grupo_raw_1}</span>\` : '';
          const c1 = item.caption_1;

          // Datos Impacto 2 (Estricto: No clona variables de Impacto 1)
          let impacto2HTML = '';
          if (totalConteo >= 2 || item.nombre_push_2 || item.usuario_raw_2 || item.grupo_raw_2 || item.caption_2) {
            const p2 = item.nombre_push_2 || 'Desconocido (2x)';
            const u2 = item.usuario_raw_2 ? \`<span class="text-[9px] text-slate-400 font-mono block truncate" title="\${item.usuario_raw_2}">JID 2: \${item.usuario_raw_2}</span>\` : '<span class="text-[9px] text-slate-600 font-mono block">JID 2: Sin registrar</span>';
            const g2 = item.grupo_raw_2 ? \`<span class="text-[9px] text-indigo-400/80 font-mono block truncate" title="\${item.grupo_raw_2}">Grupo 2: \${item.grupo_raw_2}</span>\` : '<span class="text-[9px] text-slate-600 font-mono block">Grupo 2: Sin registrar</span>';
            const c2 = item.caption_2;

            impacto2HTML = \`
              <div class="mt-2 pt-2 border-t border-slate-800/80 space-y-1">
                <span class="text-[9px] text-indigo-400 font-extrabold uppercase tracking-wider block">IMPACTO 2x</span>
                <div class="font-bold text-sky-400 truncate text-[10px]" title="\${p2}">\${p2}</div>
                \${u2}
                \${g2}
                <div class="bg-slate-900/80 p-1.5 rounded text-slate-300 text-[10px] italic border border-slate-800 max-h-10 overflow-y-auto">
                  \${c2 ? c2 : '<span class="text-slate-600">Sin texto...</span>'}
                </div>
              </div>
            \`;
          }

          return \`
            <div class="card-bg border border-slate-800 rounded-xl p-4 shadow-lg flex flex-col justify-between space-y-3 hover:border-slate-700 transition">
              <div class="flex justify-between items-center text-[10px] font-mono text-slate-400 border-b border-slate-800/80 pb-2">
                <span title="\${item.hash_largo}">\${item.hash_largo ? item.hash_largo.substring(0, 12) : 'N/A'}...</span>
                <div class="flex items-center gap-1.5">
                  \${conteoHTML}
                  \${badgeHTML}
                  <button onclick="borrarRegistro('\${item.hash_largo}')" class="text-slate-500 hover:text-amber-500 transition p-1 ml-1" title="Descartar registro">🗑️</button>
                </div>
              </div>

              <div class="flex gap-3 items-start">
                \${containerImagenes}
                
                <div class="flex-1 space-y-2 text-xs overflow-hidden">
                  <!-- Impacto 1 -->
                  <div class="space-y-1">
                    <span class="text-slate-400 text-[9px] font-bold block uppercase tracking-wider">IMPACTO 1x</span>
                    <div class="font-bold text-sky-400 truncate text-[11px]" title="\${p1}">\${p1}</div>
                    \${u1}
                    \${g1}
                    <div class="bg-slate-900 p-1.5 rounded text-slate-300 text-[10px] max-h-10 overflow-y-auto italic border border-slate-800">
                      \${c1 ? c1 : '<span class="text-slate-600">Sin texto...</span>'}
                    </div>
                  </div>

                  <!-- Impacto 2 -->
                  \${impacto2HTML}

                  <!-- Lectura IA -->
                  \${extraccionIA}

                </div>
              </div>

              <div class="text-[10px] text-slate-500 font-mono text-right pt-1 border-t border-slate-800/50">
                \${fechaTexto}
              </div>
            </div>
          \`;
        }).join('');

        document.getElementById('grid-container').innerHTML = html;
      } catch(e) { console.error(e); }
    }

    cargarInstancias();
    cargar();
    setInterval(cargar, 5000);
  </script>
</body>
</html>
  `);
});

app.listen(PORT, () => console.log(`[Panel Service] Activo en puerto ${PORT}`));
