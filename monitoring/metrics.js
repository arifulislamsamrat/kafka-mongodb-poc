const express = require('express');
const config = require('../src/config');
const logger = require('../src/utils/logger');

class Metrics {
  constructor(kafkaConsumer, mongodbService) {
    this.kafkaConsumer = kafkaConsumer;
    this.mongodbService = mongodbService;
    this.server = null;
    this.app = express();
    this.startTime = Date.now();
    this.requestCounts = new Map();
    this.setupRoutes();
    this.setupMiddleware();
  }

  setupMiddleware() {
    // Request counting middleware
    this.app.use((req, res, next) => {
      const endpoint = req.path;
      const current = this.requestCounts.get(endpoint) || 0;
      this.requestCounts.set(endpoint, current + 1);
      next();
    });
  }

  setupRoutes() {
    // Prometheus-style metrics
    this.app.get('/metrics', async (req, res) => {
      try {
        const metrics = await this.generatePrometheusMetrics();
        res.set('Content-Type', 'text/plain');
        res.send(metrics);
      } catch (error) {
        logger.error('Failed to generate metrics', { error: error.message });
        res.status(500).send('# Error generating metrics\n');
      }
    });

    // JSON metrics
    this.app.get('/metrics/json', async (req, res) => {
      try {
        const metrics = await this.generateJSONMetrics();
        res.json(metrics);
      } catch (error) {
        logger.error('Failed to generate JSON metrics', { error: error.message });
        res.status(500).json({
          error: error.message,
          timestamp: new Date().toISOString()
        });
      }
    });

    // Application-specific metrics
    this.app.get('/metrics/kafka', async (req, res) => {
      try {
        const kafkaMetrics = this.kafkaConsumer.getMetrics();
        res.json(kafkaMetrics);
      } catch (error) {
        res.status(500).json({
          error: error.message,
          timestamp: new Date().toISOString()
        });
      }
    });

    this.app.get('/metrics/mongodb', async (req, res) => {
      try {
        const mongoMetrics = this.mongodbService.getMetrics();
        res.json(mongoMetrics);
      } catch (error) {
        res.status(500).json({
          error: error.message,
          timestamp: new Date().toISOString()
        });
      }
    });

    // System metrics
    this.app.get('/metrics/system', (req, res) => {
      try {
        const systemMetrics = this.getSystemMetrics();
        res.json(systemMetrics);
      } catch (error) {
        res.status(500).json({
          error: error.message,
          timestamp: new Date().toISOString()
        });
      }
    });
  }

  async generatePrometheusMetrics() {
    const kafkaMetrics = this.kafkaConsumer.getMetrics();
    const mongoMetrics = this.mongodbService.getMetrics();
    const systemMetrics = this.getSystemMetrics();

    let output = '';

    // Help and type declarations
    output += '# HELP kafka_messages_processed_total Total number of Kafka messages processed\n';
    output += '# TYPE kafka_messages_processed_total counter\n';
    output += `kafka_messages_processed_total ${kafkaMetrics.messageCount}\n\n`;

    output += '# HELP kafka_messages_errors_total Total number of Kafka message processing errors\n';
    output += '# TYPE kafka_messages_errors_total counter\n';
    output += `kafka_messages_errors_total ${kafkaMetrics.errorCount}\n\n`;

    output += '# HELP kafka_consumer_running Whether the Kafka consumer is running\n';
    output += '# TYPE kafka_consumer_running gauge\n';
    output += `kafka_consumer_running ${kafkaMetrics.isRunning ? 1 : 0}\n\n`;

    output += '# HELP kafka_success_rate_percent Success rate of Kafka message processing\n';
    output += '# TYPE kafka_success_rate_percent gauge\n';
    output += `kafka_success_rate_percent ${kafkaMetrics.successRate}\n\n`;

    output += '# HELP mongodb_documents_inserted_total Total number of documents inserted into MongoDB\n';
    output += '# TYPE mongodb_documents_inserted_total counter\n';
    output += `mongodb_documents_inserted_total ${mongoMetrics.insertCount}\n\n`;

    output += '# HELP mongodb_errors_total Total number of MongoDB errors\n';
    output += '# TYPE mongodb_errors_total counter\n';
    output += `mongodb_errors_total ${mongoMetrics.errorCount}\n\n`;

    output += '# HELP mongodb_connected Whether MongoDB is connected\n';
    output += '# TYPE mongodb_connected gauge\n';
    output += `mongodb_connected ${mongoMetrics.isConnected ? 1 : 0}\n\n`;

    output += '# HELP mongodb_success_rate_percent Success rate of MongoDB operations\n';
    output += '# TYPE mongodb_success_rate_percent gauge\n';
    output += `mongodb_success_rate_percent ${mongoMetrics.successRate}\n\n`;

    output += '# HELP process_uptime_seconds Process uptime in seconds\n';
    output += '# TYPE process_uptime_seconds gauge\n';
    output += `process_uptime_seconds ${systemMetrics.uptime}\n\n`;

    output += '# HELP process_memory_usage_bytes Process memory usage in bytes\n';
    output += '# TYPE process_memory_usage_bytes gauge\n';
    output += `process_memory_usage_bytes{type="rss"} ${systemMetrics.memory.rss}\n`;
    output += `process_memory_usage_bytes{type="heapTotal"} ${systemMetrics.memory.heapTotal}\n`;
    output += `process_memory_usage_bytes{type="heapUsed"} ${systemMetrics.memory.heapUsed}\n`;
    output += `process_memory_usage_bytes{type="external"} ${systemMetrics.memory.external}\n\n`;

    output += '# HELP http_requests_total Total number of HTTP requests\n';
    output += '# TYPE http_requests_total counter\n';
    for (const [endpoint, count] of this.requestCounts.entries()) {
      output += `http_requests_total{endpoint="${endpoint}"} ${count}\n`;
    }
    output += '\n';

    // Circuit breaker metrics
    if (kafkaMetrics.circuitBreakerState) {
      output += '# HELP kafka_circuit_breaker_state Circuit breaker state (0=CLOSED, 1=OPEN, 2=HALF_OPEN)\n';
      output += '# TYPE kafka_circuit_breaker_state gauge\n';
      const state = kafkaMetrics.circuitBreakerState.state;
      const stateValue = state === 'CLOSED' ? 0 : state === 'OPEN' ? 1 : 2;
      output += `kafka_circuit_breaker_state ${stateValue}\n\n`;

      output += '# HELP kafka_circuit_breaker_failures Circuit breaker failure count\n';
      output += '# TYPE kafka_circuit_breaker_failures gauge\n';
      output += `kafka_circuit_breaker_failures ${kafkaMetrics.circuitBreakerState.failures}\n\n`;
    }

    if (mongoMetrics.circuitBreakerState) {
      output += '# HELP mongodb_circuit_breaker_state Circuit breaker state (0=CLOSED, 1=OPEN, 2=HALF_OPEN)\n';
      output += '# TYPE mongodb_circuit_breaker_state gauge\n';
      const state = mongoMetrics.circuitBreakerState.state;
      const stateValue = state === 'CLOSED' ? 0 : state === 'OPEN' ? 1 : 2;
      output += `mongodb_circuit_breaker_state ${stateValue}\n\n`;

      output += '# HELP mongodb_circuit_breaker_failures Circuit breaker failure count\n';
      output += '# TYPE mongodb_circuit_breaker_failures gauge\n';
      output += `mongodb_circuit_breaker_failures ${mongoMetrics.circuitBreakerState.failures}\n\n`;
    }

    return output;
  }

  async generateJSONMetrics() {
    const kafkaMetrics = this.kafkaConsumer.getMetrics();
    const mongoMetrics = this.mongodbService.getMetrics();
    const systemMetrics = this.getSystemMetrics();

    return {
      kafka: {
        ...kafkaMetrics,
        lastMessageTime: kafkaMetrics.lastMessageTime?.toISOString() || null
      },
      mongodb: mongoMetrics,
      system: systemMetrics,
      http: {
        requestCounts: Object.fromEntries(this.requestCounts)
      },
      application: {
        startTime: new Date(this.startTime).toISOString(),
        uptime: systemMetrics.uptime,
        environment: config.env
      },
      timestamp: new Date().toISOString()
    };
  }

  getSystemMetrics() {
    const memory = process.memoryUsage();
    const cpuUsage = process.cpuUsage();

    return {
      uptime: process.uptime(),
      memory: {
        rss: memory.rss,
        heapTotal: memory.heapTotal,
        heapUsed: memory.heapUsed,
        external: memory.external,
        arrayBuffers: memory.arrayBuffers
      },
      cpu: {
        user: cpuUsage.user,
        system: cpuUsage.system
      },
      process: {
        pid: process.pid,
        version: process.version,
        platform: process.platform,
        arch: process.arch
      }
    };
  }

  async getDetailedMetrics() {
    try {
      const [kafkaHealth, mongoHealth] = await Promise.all([
        this.kafkaConsumer.healthCheck(),
        this.mongodbService.healthCheck()
      ]);

      const kafkaMetrics = this.kafkaConsumer.getMetrics();
      const mongoMetrics = this.mongodbService.getMetrics();
      const systemMetrics = this.getSystemMetrics();

      return {
        health: {
          kafka: kafkaHealth,
          mongodb: mongoHealth
        },
        metrics: {
          kafka: kafkaMetrics,
          mongodb: mongoMetrics,
          system: systemMetrics
        },
        performance: {
          messagesPerSecond: this.calculateMessagesPerSecond(kafkaMetrics),
          documentsPerSecond: this.calculateDocumentsPerSecond(mongoMetrics),
          errorRate: this.calculateErrorRate(kafkaMetrics, mongoMetrics)
        },
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      throw new Error(`Failed to get detailed metrics: ${error.message}`);
    }
  }

  calculateMessagesPerSecond(kafkaMetrics) {
    const uptimeSeconds = process.uptime();
    return uptimeSeconds > 0 ? (kafkaMetrics.messageCount / uptimeSeconds).toFixed(2) : 0;
  }

  calculateDocumentsPerSecond(mongoMetrics) {
    const uptimeSeconds = process.uptime();
    return uptimeSeconds > 0 ? (mongoMetrics.insertCount / uptimeSeconds).toFixed(2) : 0;
  }

  calculateErrorRate(kafkaMetrics, mongoMetrics) {
    const totalOperations = kafkaMetrics.messageCount + mongoMetrics.insertCount;
    const totalErrors = kafkaMetrics.errorCount + mongoMetrics.errorCount;
    return totalOperations > 0 ? ((totalErrors / totalOperations) * 100).toFixed(2) : 0;
  }

  async start() {
    try {
      const port = config.monitoring.metricsPort;
      
      this.server = this.app.listen(port, () => {
        logger.info('Metrics server started', { port });
      });

      this.server.on('error', (error) => {
        logger.error('Metrics server error', { 
          error: error.message,
          port 
        });
      });

    } catch (error) {
      logger.error('Failed to start metrics server', { 
        error: error.message 
      });
      throw error;
    }
  }

  async stop() {
    try {
      if (this.server) {
        logger.info('Stopping metrics server');
        
        await new Promise((resolve, reject) => {
          this.server.close((error) => {
            if (error) {
              reject(error);
            } else {
              resolve();
            }
          });
        });

        this.server = null;
        logger.info('Metrics server stopped');
      }
    } catch (error) {
      logger.error('Error stopping metrics server', { 
        error: error.message 
      });
      throw error;
    }
  }

  reset() {
    this.requestCounts.clear();
    this.startTime = Date.now();
    logger.info('Metrics reset');
  }
}

module.exports = Metrics;