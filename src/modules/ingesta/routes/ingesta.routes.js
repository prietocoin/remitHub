const express = require('express');
const router = express.Router();
const { procesarWebhookIngesta } = require('../controllers/ingesta.controller');

router.post('/', procesarWebhookIngesta);

module.exports = router;
