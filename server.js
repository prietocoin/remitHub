require('dotenv').config(); // Asegura cargar las variables de entorno
const express = require('express');
const cors = require('cors');

// 1. Inicializar el Worker del Pipeline en segundo plano
require('./src/workers/pipeline.worker');

// 2. Importar rutas y módulos
const ingestaRoutes = require('./src/modules/ingesta/routes/ingesta.routes');
const panelRoutes = require('./src/modules/panel/routes/panel.routes');
const bullBoardRouter = require('./src/modules/panel/config/bullBoard');

const app = express();

// Middlewares globales
app.use(cors());
app.use(express.json());

// 3. Enrutamiento de la aplicación
app.use('/webhook', ingestaRoutes);       // Recepción de WhatsApp / Evolution API
app.use('/admin/queues', bullBoardRouter); // Visor gráfico Bull-Board
app.use('/', panelRoutes);                 // Dashboard y API del Panel

// 4. Encendido del servidor
const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`[remitHub 🚀] Servidor unificado activo en puerto ${PORT}`);
  console.log(`[remitHub] 📥 Webhook Ingesta: http://localhost:${PORT}/webhook`);
  console.log(`[remitHub] 📊 Bull-Board: http://localhost:${PORT}/admin/queues`);
  console.log(`[remitHub] 👁️ Dashboard: http://localhost:${PORT}/`);
});
