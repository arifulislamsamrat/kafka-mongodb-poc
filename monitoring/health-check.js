const express = require('express');
const config = require('../src/config');
const logger = require('../src/utils/logger');

class HealthCheck {
  constructor(kafkaConsumer, mongodbService) {
    this.kafkaConsumer = kafkaConsumer;
    this.mongodbService = mongodbService;
    this.server = null;
    this.app = express();
    this.setupRoutes();
  }

  setupRoutes() {
    // Basic health check
    this.app.get('/health', async (req, res) => {
      try {
        const health = await this.performHealthCheck();
        const statusCode = health.status === 'healthy' ? 200 : 503;
        res.status(statusCode).json(health);
      } catch (error) {
        logger.error('Health check failed', { error: error.message });
        res.status(503).json({
          status: 'unhealthy',
          error: error.message,
          timestamp: new Date().toISOString()
        });
      }
    });

    // Detailed health check
    this.app.get('/health/detailed', async (req, res) => {
      try {
        const detailed = await this.performDetailedHealthCheck();
        const statusCode = detailed.status === 'healthy' ? 200 : 503;
        res.status(statusCode).json(detailed);
      } catch (error) {
        logger.error('Detailed health check failed', { error: error.message });
        res.status(503).json({
          status: 'unhealthy',
          error: error.message,
          timestamp: new Date().toISOString()
        });
      }
    });

    // Component-specific health checks
    this.app.get('/health/kafka', async (req, res) => {
      try {
        const kafkaHealth = await this.kafkaConsumer.healthCheck();
        const statusCode = kafkaHealth.status === 'healthy' ? 200 : 503;
        res.status(statusCode).json(kafkaHealth);
      } catch (error) {
        res.status(503).json({
          status: 'unhealthy',
          error: error.message,
          timestamp: new Date().toISOString()
        });
      }
    });

    this.app.get('/health/mongodb', async (req, res) => {
      try {
        const mongoHealth = await this.mongodbService.healthCheck();
        const statusCode = mongoHealth.status === 'healthy' ? 200 : 503;
        res.status(statusCode).json(mongoHealth);
      } catch (error) {
        res.status(503).json({
          status: 'unhealthy',
          error: error.message,
          timestamp: new Date().toISOString()
        });
      }
    });

    // Readiness probe
    this.app.get('/ready', async (req, res) => {
      try {
        const readiness = await this.checkReadiness();
        const statusCode = readiness.ready ? 200 : 503;
        res.status(statusCode).json(readiness);
      } catch (error) {
        res.status(503).json({
          ready: false,
          error: error.message,
          timestamp: new Date().toISOString()
        });
      }
    });

    // Liveness probe
    this.app.get('/live', (req, res) => {
      res.status(200).json({
        alive: true,
        uptime: process.uptime(),
        timestamp: new Date().toISOString()
      });
    });

    // Metrics endpoint
    this.app.get('/metrics/summary', async (req, res) => {
      try {
        const metrics = await this.getMetricsSummary();
        res.status(200).json(metrics);
      } catch (error) {
        res.status(500).json({
          error: error.message,
          timestamp: new Date().toISOString()
        });
      }
    });
  }

  async performHealthCheck() {
    try {
      const [kafkaHealth, mongoHealth] = await Promise.all([
        this.kafkaConsumer.healthCheck(),
        this.mongodbService.healthCheck()
      ]);

      const overallStatus = kafkaHealth.status === 'healthy' && mongoHealth.status === 'healthy' 
        ? 'healthy' 
        : 'unhealthy';

      return {
        status: overallStatus,
        components: {
          kafka: kafkaHealth.status,
          mongodb: mongoHealth.status
        },
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      return {
        status: 'unhealthy',
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
  }

  async performDetailedHealthCheck() {
    try {
      const [kafkaHealth, mongoHealth] = await Promise.all([
        this.kafkaConsumer.healthCheck(),
        this.mongodbService.healthCheck()
      ]);

      const kafkaMetrics = this.kafkaConsumer.getMetrics();
      const mongoMetrics = this.mongodbService.getMetrics();

      const overallStatus = kafkaHealth.status === 'healthy' && mongoHealth.status === 'healthy' 
        ? 'healthy' 
        : 'unhealthy';

      return {
        status: overallStatus,
        components: {
          kafka: {
            ...kafkaHealth,
            metrics: kafkaMetrics
          },
          mongodb: {
            ...mongoHealth,
            metrics: mongoMetrics
          }
        },
        system: {
          uptime: process.uptime(),
          memory: process.memoryUsage(),
          cpuUsage: process.cpuUsage(),
          nodeVersion: process.version,
          platform: process.platform
        },
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      return {
        status: 'unhealthy',
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
  }

  async checkReadiness() {
    try {
      // Check if all components are ready to serve traffic
      const kafkaMetrics = this.kafkaConsumer.getMetrics();
      const mongoMetrics = this.mongodbService.getMetrics();

      const kafkaReady = kafkaMetrics.isRunning;
      const mongoReady = mongoMetrics.isConnected;

      return {
        ready: kafkaReady && mongoReady,
        components: {
          kafka: kafkaReady,
          mongodb: mongoReady
        },
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      return {
        ready: false,
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
  }

  async getMetricsSummary() {
    try {
      const kafkaMetrics = this.kafkaConsumer.getMetrics();
      const mongoMetrics = this.mongodbService.getMetrics();

      return {
        kafka: {
          messagesProcessed: kafkaMetrics.messageCount,
          errors: kafkaMetrics.errorCount,
          successRate: kafkaMetrics.successRate,
          isRunning: kafkaMetrics.isRunning,
          lastMessageTime: kafkaMetrics.lastMessageTime
        },
        mongodb: {
          documentsInserted: mongoMetrics.insertCount,
          errors: mongoMetrics.errorCount,
          successRate: mongoMetrics.successRate,
          isConnected: mongoMetrics.isConnected
        },
        system: {
          uptime: process.uptime(),
          memoryUsage: process.memoryUsage(),
          timestamp: new Date().toISOString()
        }
      };
    } catch (error) {
      throw new Error(`Failed to get metrics summary: ${error.message}`);
    }
  }

  async start() {
    try {
      const port = config.monitoring.healthCheckPort;
      
      this.server = this.app.listen(port, () => {
        logger.info('Health check server started', { port });
      });

      this.server.on('error', (error) => {
        logger.error('Health check server error', { 
          error: error.message,
          port 
        });
      });

    } catch (error) {
      logger.error('Failed to start health check server', { 
        error: error.message 
      });
      throw error;
    }
  }

  async stop() {
    try {
      if (this.server) {
        logger.info('Stopping health check server');
        
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
        logger.info('Health check server stopped');
      }
    } catch (error) {
      logger.error('Error stopping health check server', { 
        error: error.message 
      });
      throw error;
    }
  }
}

module.exports = HealthCheck;