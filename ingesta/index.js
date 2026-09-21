const app = require('./server');

const PORT = process.env.PORT || 3000;
// En ingesta/server.js (o el controlador que recibe el webhook)
await pool.query(`INSERT INTO impactos_raw (...) VALUES (...)`);

// Agregar a la cola SOLO DESPUÉS de guardar en la BD
await colaValidador.add('validar-impacto', payload);
app.listen(PORT, '0.0.0.0', () => {
  console.log(`[Ingesta] 🟢 Servidor HTTP de Ingesta Ciega activo en el puerto ${PORT}`);
});
