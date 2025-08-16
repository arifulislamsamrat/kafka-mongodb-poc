const config = require('./config');
const logger = require('./utils/logger');
const KafkaConsumerService = require('./services/kafkaConsumer');
const MongoDBService = require('./services/mongodbService');
const MessageProcessor = require('./processors/messageProcessor');
const HealthCheck = require('../monitoring/health-check');
const Metrics = require('../monitoring/metrics');

class KafkaMongoDBApp {
  constructor() {
    this.kafkaConsumer = new KafkaConsumerService();
    this.mongodbService = new MongoDBService();
    this.messageProcessor = new MessageProcessor(this.mongodbService);
    this.healthCheck = new HealthCheck(this.kafkaConsumer, this.mongodbService);
    this.metrics = new Metrics(this.kafkaConsumer, this.mongodbService);
    this.isShuttingDown = false;
  }

  async initialize() {
    try {
      logger.appLog('Initializing Kafka-MongoDB application');

      // Initialize MongoDB service
      await this.mongodbService.initialize();
      
      // Initialize Kafka consumer
      await this.kafkaConsumer.initialize();
      await this.kafkaConsumer.connect();
      await this.kafkaConsumer.subscribe();

      // Register message handlers
      this.registerMessageHandlers();

      // Start health check and metrics if enabled
      if (config.monitoring.enableHealthCheck) {
        await this.healthCheck.start();
      }

      if (config.monitoring.enableMetrics) {
        await this.metrics.start();
      }

      // Setup graceful shutdown
      this.setupGracefulShutdown();

      logger.appLog('Application initialized successfully');
    } catch (error) {
      logger.error('Failed to initialize application', { 
        error: error.message 
      });
      throw error;
    }
  }

  registerMessageHandlers() {
    // Default message handler
    this.kafkaConsumer.registerMessageHandler('default', async (message) => {
      await this.messageProcessor.processMessage(message);
    });

    // User event handlers
    this.kafkaConsumer.registerMessageHandler('user_login', async (message) => {
      await this.messageProcessor.processUserEvent(message, 'login');
    });

    this.kafkaConsumer.registerMessageHandler('user_logout', async (message) => {
      await this.messageProcessor.processUserEvent(message, 'logout');
    });

    this.kafkaConsumer.registerMessageHandler('user_purchase', async (message) => {
      await this.messageProcessor.processUserEvent(message, 'purchase');
    });

    // Add more handlers as needed
    logger.appLog('Message handlers registered');
  }

  async start() {
    try {
      logger.appLog('Starting Kafka-MongoDB application');

      // Initialize all components
      await this.initialize();

      // Start consuming messages
      await this.kafkaConsumer.startConsuming();

      logger.appLog('Application started successfully');
      
      // Log startup metrics
      this.logStartupInfo();

    } catch (error) {
      logger.error('Failed to start application', { 
        error: error.message 
      });
      await this.shutdown();
      process.exit(1);
    }
  }

  async shutdown() {
    if (this.isShuttingDown) {
      logger.warn('Shutdown already in progress');
      return;
    }

    this.isShuttingDown = true;
    logger.appLog('Starting graceful shutdown');

    try {
      // Stop health check and metrics
      if (this.healthCheck) {
        await this.healthCheck.stop();
      }

      if (this.metrics) {
        await this.metrics.stop();
      }

      // Stop Kafka consumer
      if (this.kafkaConsumer) {
        await this.kafkaConsumer.stop();
        await this.kafkaConsumer.disconnect();
      }

      // Disconnect from MongoDB
      if (this.mongodbService) {
        await this.mongodbService.disconnect();
      }

      logger.appLog('Graceful shutdown completed');
    } catch (error) {
      logger.error('Error during shutdown', { 
        error: error.message 
      });
    }
  }

  setupGracefulShutdown() {
    const signals = ['SIGINT', 'SIGTERM', 'SIGQUIT'];
    
    signals.forEach(signal => {
      process.on(signal, async () => {
        logger.appLog(`Received ${signal}, initiating graceful shutdown`);
        await this.shutdown();
        process.exit(0);
      });
    });

    process.on('uncaughtException', async (error) => {
      logger.error('Uncaught exception', { 
        error: error.message,
        stack: error.stack
      });
      await this.shutdown();
      process.exit(1);
    });

    process.on('unhandledRejection', async (reason, promise) => {
      logger.error('Unhandled promise rejection', { 
        reason: reason?.message || reason,
        promise: promise.toString()
      });
      await this.shutdown();
      process.exit(1);
    });
  }

  logStartupInfo() {
    const startupInfo = {
      nodeVersion: process.version,
      platform: process.platform,
      architecture: process.arch,
      memory: process.memoryUsage(),
      environment: config.env,
      kafka: {
        brokers: config.kafka.brokers,
        groupId: config.kafka.groupId,
        topic: config.kafka.topic
      },
      mongodb: {
        database: config.mongodb.dbName,
        collection: config.mongodb.collectionName
      },
      monitoring: {
        healthCheck: config.monitoring.enableHealthCheck,
        metrics: config.monitoring.enableMetrics,
        healthCheckPort: config.monitoring.healthCheckPort,
        metricsPort: config.monitoring.metricsPort
      }
    };

    logger.appLog('Application startup information', startupInfo);
  }

  async getStatus() {
    try {
      const kafkaHealth = await this.kafkaConsumer.healthCheck();
      const mongoHealth = await this.mongodbService.healthCheck();
      const kafkaMetrics = this.kafkaConsumer.getMetrics();
      const mongoMetrics = this.mongodbService.getMetrics();

      return {
        status: kafkaHealth.status === 'healthy' && mongoHealth.status === 'healthy' ? 'healthy' : 'unhealthy',
        components: {
          kafka: kafkaHealth,
          mongodb: mongoHealth
        },
        metrics: {
          kafka: kafkaMetrics,
          mongodb: mongoMetrics
        },
        application: {
          isShuttingDown: this.isShuttingDown,
          uptime: process.uptime(),
          memory: process.memoryUsage(),
          timestamp: new Date().toISOString()
        }
      };
    } catch (error) {
      return {
        status: 'unhealthy',
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
  }
}

// Create and export app instance
const app = new KafkaMongoDBApp();

// Start the application if this file is run directly
if (require.main === module) {
  app.start().catch(error => {
    logger.error('Application startup failed', { 
      error: error.message,
      stack: error.stack
    });
    process.exit(1);
  });
}

module.exports = app;