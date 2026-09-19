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

// Función para reparar y construir URLs públicas válidas de R2 al vuelo
function resolverUrlImagen(rawPathOrUrl) {
  if (!rawPathOrUrl) return null;
  
  const r2Domain = (process.env.R2_PUBLIC_DOMAIN || '').replace(/\/$/, '');

  // 1. Extraer la ruta de la llave R2 (ej: "comprobantes/9AC9F...jpg")
  const keyMatch = rawPathOrUrl.match(/comprobantes\/[^\s"']+/);
  
  if (keyMatch && r2Domain) {
    return `${r2Domain}/${keyMatch[0]}`;
  }

  // 2. Si ya es una URL válida y no tiene dominios erróneos
  if (rawPathOrUrl.startsWith('http') && !rawPathOrUrl.includes('pub-xxxx') && !rawPathOrUrl.includes('automat-panel')) {
    return rawPathOrUrl;
  }

  // 3. Fallback con r2Domain limpiando el host previo
  if (r2Domain) {
    const cleanKey = rawPathOrUrl.replace(/^https?:\/\/[^\/]+\//, '');
    return `${r2Domain}/${cleanKey}`;
  }

  return rawPathOrUrl;
}

// 1. ENDPOINTS API

// Obtener la lista de todas las instancias activas
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

// Obtener comprobantes procesando dinámicamente las URLs de R2
app.get('/api/comprobantes', async (req, res) => {
  try {
    const instanciaTarget = req.query.instancia || 'JAIRO';

    const query = `
      SELECT 
        r.hash_largo,
        r.estado,
        COALESCE(c.url_r2, r.url_r2, r.url_imagen, r.key_r2) as url_raw_db,
        r.timestamp_msg,
        r.nombre_push,
        r.usuario_raw,
        r.grupo_raw,
        r.caption,
        r.instancia,
        r.conteo,
        c.monto,
        c.moneda,
        c.banco,
        c.referencia,
        c.estado_ia
      FROM registros_raw r
      LEFT JOIN comprobantes_raw c ON r.hash_largo = c.hash_largo
      WHERE LOWER(r.instancia) = LOWER($1)
      ORDER BY r.timestamp_msg DESC
      LIMIT 60
    `;
    const { rows } = await pool.query(query, [instanciaTarget]);

    // Sanitizar y reparar cada URL antes de enviarla al frontend
    const itemsFormateados = rows.map(row => ({
      ...row,
      url_imagen: resolverUrlImagen(row.url_raw_db)
    }));

    res.json(itemsFormateados);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Descarte lógico (Soft-Delete)
app.delete('/api/comprobantes/:hash', async (req, res) => {
  const { hash } = req.params;
  try {
    await pool.query(`UPDATE registros_raw SET estado = 'DESCARTADO' WHERE hash_largo = $1`, [hash]);
    res.json({ success: true, hash });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 2. DASHBOARD WEB CON SELECTOR DE INSTANCIAS
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
      window.history.pushState({}, '', '?instancia=' + encodeURIComponent(nuevaInstancia));
      cargar();
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
        const res = await fetch('/api/comprobantes?instancia=' + encodeURIComponent(INSTANCIA_ACTUAL));
        const items = await res.json();

        if (!Array.isArray(items)) {
          document.getElementById('grid-container').innerHTML = \`<div class="col-span-full text-center py-12 text-rose-400 font-mono text-xs">⚠️ Error: \${items.error || 'Desconocido'}</div>\`;
          return;
        }

        document.getElementById('c-total').innerText = items.length;

        if (items.length === 0) {
          document.getElementById('grid-container').innerHTML = \`<div class="col-span-full text-center py-12 text-slate-500">No hay registros recientes para \${INSTANCIA_ACTUAL.toUpperCase()}.</div>\`;
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

          let imgHTML = '<div class="w-full h-full flex items-center justify-center text-[10px] text-slate-600 font-mono text-center px-2">Sin Imagen<br>(Esperando 2x)</div>';
          if (item.url_imagen) {
            imgHTML = \`<img src="\${item.url_imagen}" class="w-full h-full object-cover cursor-pointer hover:scale-105 transition" onclick="window.open(this.src)" title="Click para expandir" onerror="this.onerror=null; this.parentElement.innerHTML='<div class=\\'w-full h-full flex items-center justify-center text-[9px] text-rose-400 font-mono text-center px-1\\'>Error de Carga R2</div>';"/>\`;
          }

          let extraccionIA = '';
          if (item.monto || item.banco) {
            extraccionIA = \`
              <div class="mt-2 p-1.5 bg-emerald-900/20 border border-emerald-800/40 rounded flex flex-col gap-0.5">
                <span class="text-[9px] text-emerald-500 font-bold uppercase tracking-wider">Lectura IA</span>
                <span class="text-[11px] text-emerald-300"><strong>\${item.monto || '0'} \${item.moneda || ''}</strong> - \${item.banco || 'N/A'}</span>
                <span class="text-[10px] text-emerald-400/70 font-mono">Ref: \${item.referencia || 'N/A'}</span>
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
                <div class="w-28 h-44 bg-slate-900 rounded-lg overflow-hidden border border-slate-800 flex-shrink-0">
                  \${imgHTML}
                </div>
                
                <div class="flex-1 space-y-2 text-xs overflow-hidden">
                  <div class="border-b border-slate-800/50 pb-1.5">
                    <span class="text-slate-400 text-[10px] block font-semibold mb-0.5">Remitente:</span>
                    <div class="font-bold text-sky-400 truncate text-[11px]" title="\${item.nombre_push || item.usuario_raw}">
                      \${item.nombre_push || item.usuario_raw || 'Desconocido'}
                    </div>
                  </div>
                  
                  <div>
                    <span class="text-slate-500 text-[10px] block mb-0.5 font-semibold">Texto (Caption):</span>
                    <div class="bg-slate-900 p-2 rounded text-slate-300 text-[11px] max-h-12 overflow-y-auto italic border border-slate-800">
                      \${item.caption ? item.caption : '<span class="text-slate-600">Sin texto...</span>'}
                    </div>
                  </div>
                  
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
