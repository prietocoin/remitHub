const express = require('express');
const router = require('./src/routes/router');
const queuesModule = require('./src/routes/queues.router');

const app = express();
app.use(express.json());

// 1. Montar el panel de monitoreo de BullMQ
app.use(queuesModule.path, queuesModule.router);

// 2. Rutas principales del panel
app.use('/', router);

const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`[Panel Service] 🟢 Servidor activo en el puerto ${PORT}`);
  console.log(`[Panel Service] 📊 Dashboard de colas listo en: /admin/queues`);
});
