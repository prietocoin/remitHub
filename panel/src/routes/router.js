const express = require('express');
const router = express.Router();

const { renderDashboard } = require('../controllers/dashboard.controller');
const { getInstancias, getComprobantes, deleteComprobante } = require('../controllers/api.controller');

// Dashboard Principal
router.get('/', renderDashboard);

// Endpoints de API
router.get('/api/instancias', getInstancias);
router.get('/api/comprobantes', getComprobantes);
router.delete('/api/comprobantes/:hash', deleteComprobante);

module.exports = router;
