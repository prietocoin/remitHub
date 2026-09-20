function renderDashboard(req, res) {
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

          const p1 = item.nombre_push_1 || 'Desconocido';
          const u1 = item.usuario_raw_1 ? \`<span class="text-[9px] text-slate-400 font-mono block truncate" title="\${item.usuario_raw_1}">JID 1: \${item.usuario_raw_1}</span>\` : '';
          const g1 = item.grupo_raw_1 ? \`<span class="text-[9px] text-indigo-400/80 font-mono block truncate" title="\${item.grupo_raw_1}">Grupo 1: \${item.grupo_raw_1}</span>\` : '';
          const c1 = item.caption_1;

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
                  <div class="space-y-1">
                    <span class="text-slate-400 text-[9px] font-bold block uppercase tracking-wider">IMPACTO 1x</span>
                    <div class="font-bold text-sky-400 truncate text-[11px]" title="\${p1}">\${p1}</div>
                    \${u1}
                    \${g1}
                    <div class="bg-slate-900 p-1.5 rounded text-slate-300 text-[10px] max-h-10 overflow-y-auto italic border border-slate-800">
                      \${c1 ? c1 : '<span class="text-slate-600">Sin texto...</span>'}
                    </div>
                  </div>

                  \${impacto2HTML}
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
}

module.exports = { renderDashboard };
