const app = require('./server');

const PORT = process.env.PORT || 3000;

app.listen(PORT, '0.0.0.0', () => {
  console.log(`[Ingesta] 🟢 Servidor HTTP de Ingesta Ciega activo en el puerto ${PORT}`);
});
