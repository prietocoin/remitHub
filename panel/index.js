const express = require('express');
const router = require('./src/routes/router');

const app = express();
app.use(express.json());

app.use('/', router);

const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`[Panel Service] 🟢 Servidor activo en el puerto ${PORT}`);
});
