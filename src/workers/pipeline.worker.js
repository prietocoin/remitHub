const { Worker } = require('bullmq');
const redisConfig = require('../config/redis');

// Importación de servicios de cada módulo
const { procesarValidadorBinomio } = require('../modules/validador/services/validador.service');
const { procesarExtraccionIA } = require('../modules/extractor/services/extractor.service');
const { procesarEnsambladoDatos } = require('../modules/ensamblador/services/ensamblador.service');
const { procesarDistribucion } = require('../modules/distribuidor/services/distribuidor.service');

const worker = new Worker('cola-pipeline', async (job) => {
  const { name, data } = job;
  const hashLargo = data.hash_largo || data.hashLargo;
  const hashCorto = hashLargo ? hashLargo.slice(-8) : 'DESCONOCIDO';

  console.log(`[Pipeline Worker ⚙️] Job '${name}' recibido para Hash: ${hashCorto}`);

  try {
    // CASO A: Re-lectura solicitada desde el Panel (/api/comprobantes/:hash/releer)
    if (name === 'releer-ia') {
      console.log(`[Pipeline 🔄] Ejecutando Re-lectura de IA para Hash: ${hashCorto}`);
      
      // 1. Extractor (IA)
      const datosIA = await procesarExtraccionIA(data);
      
      // 2. Ensamblador (PostgreSQL)
      await procesarEnsambladoDatos({
        hash_largo: data.hash_largo,
        instancia: data.instancia,
        datos_ia: datosIA
      });

      // 3. Distribuidor (Webhook Final)
      await procesarDistribucion({
        hash_largo: data.hash_largo,
        instancia: data.instancia
      });

      console.log(`[Pipeline ✅] Re-lectura completada con éxito para Hash: ${hashCorto}`);
      return;
    }

    // CASO B: Flujo Principal en Tiempo Real (Ingesta -> Validador Binomio 2x)
    
    // 1. Validador: R2 + Incremento de conteo (Binomio 2x)
    const resultadoValidador = await procesarValidadorBinomio(data);

    // Si aún no alcanza el Binomio (1x), se detiene la secuencia aquí
    if (!resultadoValidador || !resultadoValidador.procesarIA) {
      console.log(`[Pipeline ⏳] Hash ${hashCorto} en espera del binomio (conteo actual: ${resultadoValidador?.conteo || 1}x).`);
      return;
    }

    // 2. Extractor: Descarga de R2 e inferencia con Gemini
    console.log(`[Pipeline 🧠] Binomio 2x alcanzado. Ejecutando Extractor IA para Hash: ${hashCorto}`);
    const datosIA = await procesarExtraccionIA({
      hash_largo: resultadoValidador.hashLargo,
      url_r2: resultadoValidador.urlR2,
      caption: data.caption
    });

    // 3. Ensamblador: Transacción SQL y actualización en BD
    console.log(`[Pipeline 🧱] Asentando lectura en base de datos para Hash: ${hashCorto}`);
    await procesarEnsambladoDatos({
      hash_largo: resultadoValidador.hashLargo,
      instancia: resultadoValidador.instancia,
      datos_ia: datosIA
    });

    // 4. Distribuidor: Construcción del Payload Maestro y envío de Webhook
    console.log(`[Pipeline 🚀] Despachando evento final para Hash: ${hashCorto}`);
    await procesarDistribucion({
      hash_largo: resultadoValidador.hashLargo,
      instancia: resultadoValidador.instancia
    });

    console.log(`[Pipeline 🎉] Flujo completo ejecutado exitosamente para Hash: ${hashCorto}`);

  } catch (error) {
    console.error(`[Pipeline ERROR ❌] Falló el procesamiento del Hash ${hashCorto}:`, error.message);
    throw error; // Permite a BullMQ gestionar los reintentos automáticos
  }
}, {
  connection: redisConfig,
  concurrency: 5 // Procesamiento simultáneo de 5 comprobantes
});

// Eventos de monitoreo
worker.on('completed', (job) => {
  console.log(`[Pipeline Evento] Job ${job.id} completado con éxito.`);
});

worker.on('failed', (job, err) => {
  console.error(`[Pipeline Evento] Job ${job?.id} falló tras reintentos: ${err.message}`);
});

worker.on('error', (err) => {
  console.error('[Pipeline Error Crítico]', err.message);
});

console.log('[Pipeline Worker] 🟢 Worker unificado activo y escuchando en "cola-pipeline"...');

module.exports = worker;
