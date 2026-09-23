const express = require('express');
const router = express.Router();

const { renderDashboard } = require('../controllers/dashboard.controller');
// Línea 5: importar la función junto con las demás
const { getInstancias, getComprobantes, deleteComprobante, releerIA } = require('../controllers/api.controller');

// Dashboard Principal
router.get('/', renderDashboard);

// Endpoints de API
router.get('/api/instancias', getInstancias);
router.get('/api/comprobantes', getComprobantes);
router.delete('/api/comprobantes/:hash', deleteComprobante);
// Línea 15: usar la función directamente
router.post('/api/comprobantes/:hash/releer', releerIA);

module.exports = router;
