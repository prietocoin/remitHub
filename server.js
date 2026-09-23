const express = require('express');
const cors = require('cors');

// Importación de submódulos y configuraciones
const ingestaRoutes = require('./src/modules/ingesta/routes/ingesta.routes');
const panelRoutes = require('./src/modules/panel/routes/panel.routes');
const bullBoardRouter = require('./src/modules/panel/config/bullBoard');

const app = express();

// Middlewares globales
app.use(cors());
app.use(express.json());

// 1. Ruta para recepción de Webhooks de WhatsApp (Evolution API / Ingesta)
app.use('/webhook', ingestaRoutes);

// 2. Tablero de monitoreo visual de colas (Bull-Board)
app.use('/admin/queues', bullBoardRouter);

// 3. Panel de control visual y API REST del Panel
app.use('/', panelRoutes);

const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`[remitHub 🚀] Servidor unificado activo en el puerto ${PORT}`);
  console.log(`[remitHub] 📥 Webhook de Ingesta en: http://localhost:${PORT}/webhook`);
  console.log(`[remitHub] 📊 Bull-Board activo en: http://localhost:${PORT}/admin/queues`);
  console.log(`[remitHub] 👁️ Dashboard activo en: http://localhost:${PORT}/`);
});
// remitHub/server.js
require('./src/workers/pipeline.worker');
