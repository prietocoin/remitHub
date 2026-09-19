const app = require('./server');
// Iniciar la escucha del worker en segundo plano
require('./src/workers/download.worker');

const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`[Ingesta] 🟢 Servidor HTTP y Worker activos en el puerto ${PORT}`);
});
