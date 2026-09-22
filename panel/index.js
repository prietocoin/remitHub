const express = require('express');
const cors = require('cors'); // <-- AGREGADO
const mainRouter = require('./src/routes/router');
const queuesRouter = require('./src/routes/queues.router');

const app = express();

// Habilitar CORS para permitir peticiones desde el visor web (reme-jz)
app.use(cors()); // <-- AGREGADO
app.use(express.json());

// Montar la ruta de Bull-Board de forma directa
app.use('/admin/queues', queuesRouter);

// Montar el resto de la aplicación
app.use('/', mainRouter);

const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`[Panel Service] 🟢 Servidor activo en puerto ${PORT}`);
  console.log(`[Panel Service] 📊 Bull-Board listo en /admin/queues`);
});
