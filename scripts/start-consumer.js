#!/usr/bin/env node

const app = require('../src/app');
const logger = require('../src/utils/logger');
const config = require('../src/config');

class ConsumerManager {
  constructor() {
    this.app = app;
    this.isRunning = false;
    this.startTime = Date.now();
    this.options = {};
    this.statsInterval = null;
    this.healthCheckInterval = null;
  }

  async start() {
    try {
      // Parse command line arguments
      this.options = this.parseCommandLineArgs();
      
      logger.info('Starting Kafka to MongoDB consumer with options', this.options);
      
      // Apply options to configuration if needed
      this.applyOptions();
      
      // Start the main application
      await this.app.start();
      this.isRunning = true;
      
      // Start monitoring if enabled
      if (this.options.enableStats) {
        this.startStatsReporting();
      }
      
      if (this.options.enableHealthCheck) {
        this.startHealthChecking();
      }
      
      logger.info('Consumer started successfully', {
        pid: process.pid,
        uptime: this.getUptime(),
        options: this.options
      });
      
      // Auto-stop after duration if specified
      if (this.options.duration > 0) {
        setTimeout(() => {
          this.stop();
        }, this.options.duration * 1000);
      }
      
    } catch (error) {
      logger.error('Failed to start consumer', { 
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  parseCommandLineArgs() {
    const args = process.argv.slice(2);
    const options = {
      duration: 0, // seconds (0 = infinite)
      enableStats: true,
      statsInterval: 30, // seconds
      enableHealthCheck: true,
      healthCheckInterval: 60, // seconds
      logLevel: config.app.logLevel,
      maxRetries: config.app.maxRetries,
      batchSize: config.app.batchSize,
      resetOffsets: false,
      seekToEnd: false,
      seekToBeginning: false,
      pauseOnError: false,
      dryRun: false,
      verbose: false
    };

    for (let i = 0; i < args.length; i++) {
      switch (args[i]) {
        case '--duration':
        case '-d':
          options.duration = parseInt(args[i + 1]) || 0;
          i++;
          break;
        case '--stats':
        case '-s':
          options.enableStats = true;
          break;
        case '--no-stats':
          options.enableStats = false;
          break;
        case '--stats-interval':
          options.statsInterval = parseInt(args[i + 1]) || 30;
          i++;
          break;
        case '--health':
        case '-h':
          options.enableHealthCheck = true;
          break;
        case '--no-health':
          options.enableHealthCheck = false;
          break;
        case '--health-interval':
          options.healthCheckInterval = parseInt(args[i + 1]) || 60;
          i++;
          break;
        case '--log-level':
        case '-l':
          options.logLevel = args[i + 1] || 'info';
          i++;
          break;
        case '--max-retries':
          options.maxRetries = parseInt(args[i + 1]) || 3;
          i++;
          break;
        case '--batch-size':
        case '-b':
          options.batchSize = parseInt(args[i + 1]) || 100;
          i++;
          break;
        case '--reset-offsets':
          options.resetOffsets = true;
          break;
        case '--seek-to-end':
          options.seekToEnd = true;
          break;
        case '--seek-to-beginning':
          options.seekToBeginning = true;
          break;
        case '--pause-on-error':
          options.pauseOnError = true;
          break;
        case '--dry-run':
          options.dryRun = true;
          break;
        case '--verbose':
        case '-v':
          options.verbose = true;
          options.logLevel = 'debug';
          break;
        case '--help':
          this.printUsage();
          process.exit(0);
          break;
        default:
          if (args[i].startsWith('--')) {
            logger.warn('Unknown option', { option: args[i] });
          }
          break;
      }
    }

    return options;
  }

  printUsage() {
    console.log(`
Usage: node scripts/start-consumer.js [OPTIONS]

Options:
  -d, --duration <seconds>        Run for specified duration (0 = infinite, default: 0)
  -s, --stats                     Enable statistics reporting (default: true)
  --no-stats                      Disable statistics reporting
  --stats-interval <seconds>      Statistics reporting interval (default: 30)
  -h, --health                    Enable health checking (default: true)
  --no-health                     Disable health checking
  --health-interval <seconds>     Health check interval (default: 60)
  -l, --log-level <level>         Log level: error|warn|info|debug (default: info)
  --max-retries <number>          Maximum retry attempts (default: 3)
  -b, --batch-size <number>       Batch processing size (default: 100)
  --reset-offsets                 Reset consumer group offsets to beginning
  --seek-to-end                   Seek to end of topics (skip existing messages)
  --seek-to-beginning             Seek to beginning of topics
  --pause-on-error                Pause consumer on processing errors
  --dry-run                       Process messages but don't store in MongoDB
  -v, --verbose                   Enable verbose logging (debug level)
  --help                          Show this help

Examples:
  # Start consumer with default settings
  node scripts/start-consumer.js
  
  # Run for 5 minutes with verbose logging
  node scripts/start-consumer.js --duration 300 --verbose
  
  # Start fresh by resetting offsets
  node scripts/start-consumer.js --reset-offsets
  
  # Process only new messages
  node scripts/start-consumer.js --seek-to-end
  
  # Dry run mode (don't save to MongoDB)
  node scripts/start-consumer.js --dry-run --verbose
  
  # Production mode with minimal logging
  node scripts/start-consumer.js --log-level warn --no-stats
    `);
  }

  applyOptions() {
    // Update logger level if specified
    if (this.options.logLevel !== config.app.logLevel) {
      logger.level = this.options.logLevel;
      logger.info('Log level changed', { 
        from: config.app.logLevel, 
        to: this.options.logLevel 
      });
    }

    // Override config values
    if (this.options.maxRetries !== config.app.maxRetries) {
      config.app.maxRetries = this.options.maxRetries;
      logger.info('Max retries changed', { 
        maxRetries: this.options.maxRetries 
      });
    }

    if (this.options.batchSize !== config.app.batchSize) {
      config.app.batchSize = this.options.batchSize;
      logger.info('Batch size changed', { 
        batchSize: this.options.batchSize 
      });
    }
  }

  async handleSpecialModes() {
    const KafkaConsumerService = require('../src/services/kafkaConsumer');
    const consumer = new KafkaConsumerService();
    
    try {
      await consumer.initialize();
      await consumer.connect();

      if (this.options.resetOffsets) {
        logger.info('Resetting consumer group offsets...');
        await consumer.resetOffsets(config.kafka.groupId, config.kafka.topic);
        logger.info('Offsets reset successfully');
      }

      if (this.options.seekToEnd) {
        logger.info('Seeking to end of topics...');
        await consumer.seek(config.kafka.topic, 0, '-1'); // -1 means end
        logger.info('Seeked to end successfully');
      }

      if (this.options.seekToBeginning) {
        logger.info('Seeking to beginning of topics...');
        await consumer.seek(config.kafka.topic, 0, '0'); // 0 means beginning
        logger.info('Seeked to beginning successfully');
      }

      await consumer.disconnect();
    } catch (error) {
      logger.error('Failed to handle special modes', { 
        error: error.message 
      });
      throw error;
    }
  }

  startStatsReporting() {
    this.statsInterval = setInterval(async () => {
      if (this.isRunning) {
        try {
          await this.reportStatistics();
        } catch (error) {
          logger.error('Failed to report statistics', { 
            error: error.message 
          });
        }
      }
    }, this.options.statsInterval * 1000);

    logger.info('Statistics reporting started', { 
      interval: this.options.statsInterval 
    });
  }

  startHealthChecking() {
    this.healthCheckInterval = setInterval(async () => {
      if (this.isRunning) {
        try {
          await this.performHealthCheck();
        } catch (error) {
          logger.error('Health check failed', { 
            error: error.message 
          });
          
          if (this.options.pauseOnError) {
            logger.warn('Pausing consumer due to health check failure');
            // Implementation would pause the consumer here
          }
        }
      }
    }, this.options.healthCheckInterval * 1000);

    logger.info('Health checking started', { 
      interval: this.options.healthCheckInterval 
    });
  }

  async reportStatistics() {
    try {
      const status = await this.app.getStatus();
      const uptime = this.getUptime();
      
      const stats = {
        uptime: uptime,
        status: status.status,
        kafka: {
          status: status.components.kafka.status,
          messagesProcessed: status.metrics.kafka.messageCount,
          errors: status.metrics.kafka.errorCount,
          successRate: status.metrics.kafka.successRate,
          isRunning: status.metrics.kafka.isRunning,
          lastMessageTime: status.metrics.kafka.lastMessageTime
        },
        mongodb: {
          status: status.components.mongodb.status,
          documentsInserted: status.metrics.mongodb.insertCount,
          errors: status.metrics.mongodb.errorCount,
          successRate: status.metrics.mongodb.successRate,
          isConnected: status.metrics.mongodb.isConnected
        },
        system: {
          memory: status.application.memory,
          uptime: status.application.uptime
        }
      };

      // Calculate rates
      const uptimeSeconds = uptime.seconds;
      if (uptimeSeconds > 0) {
        stats.rates = {
          messagesPerSecond: (stats.kafka.messagesProcessed / uptimeSeconds).toFixed(2),
          documentsPerSecond: (stats.mongodb.documentsInserted / uptimeSeconds).toFixed(2),
          errorRate: ((stats.kafka.errors + stats.mongodb.errors) / uptimeSeconds).toFixed(2)
        };
      }

      logger.info('Consumer Statistics', stats);

      // Log warnings for concerning metrics
      if (parseFloat(stats.kafka.successRate) < 95) {
        logger.warn('Low Kafka success rate detected', { 
          successRate: stats.kafka.successRate 
        });
      }

      if (parseFloat(stats.mongodb.successRate) < 95) {
        logger.warn('Low MongoDB success rate detected', { 
          successRate: stats.mongodb.successRate 
        });
      }

      const memoryUsageMB = stats.system.memory.heapUsed / 1024 / 1024;
      if (memoryUsageMB > 512) {
        logger.warn('High memory usage detected', { 
          memoryUsageMB: memoryUsageMB.toFixed(2)
        });
      }

    } catch (error) {
      logger.error('Failed to collect statistics', { 
        error: error.message 
      });
    }
  }

  async performHealthCheck() {
    try {
      const status = await this.app.getStatus();
      
      if (status.status !== 'healthy') {
        logger.warn('Application health check failed', {
          status: status.status,
          kafka: status.components.kafka.status,
          mongodb: status.components.mongodb.status
        });
        
        if (this.options.pauseOnError) {
          // Could implement pausing logic here
          logger.warn('Health check failure - pause on error enabled');
        }
      } else {
        logger.debug('Health check passed', {
          kafka: status.components.kafka.status,
          mongodb: status.components.mongodb.status
        });
      }
    } catch (error) {
      logger.error('Health check error', { 
        error: error.message 
      });
    }
  }

  getUptime() {
    const uptimeMs = Date.now() - this.startTime;
    const seconds = Math.floor(uptimeMs / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);

    return {
      milliseconds: uptimeMs,
      seconds: seconds,
      formatted: `${days}d ${hours % 24}h ${minutes % 60}m ${seconds % 60}s`
    };
  }

  async stop() {
    try {
      logger.info('Stopping consumer...');
      this.isRunning = false;
      
      // Clear intervals
      if (this.statsInterval) {
        clearInterval(this.statsInterval);
        this.statsInterval = null;
      }
      
      if (this.healthCheckInterval) {
        clearInterval(this.healthCheckInterval);
        this.healthCheckInterval = null;
      }
      
      // Stop the main application
      await this.app.shutdown();
      
      // Final statistics
      const uptime = this.getUptime();
      logger.info('Consumer stopped - Final Report', {
        totalUptime: uptime.formatted,
        processId: process.pid
      });
      
      process.exit(0);
    } catch (error) {
      logger.error('Error stopping consumer', { 
        error: error.message 
      });
      process.exit(1);
    }
  }

  setupGracefulShutdown() {
    const signals = ['SIGINT', 'SIGTERM', 'SIGQUIT'];
    
    signals.forEach(signal => {
      process.on(signal, async () => {
        logger.info(`Received ${signal}, initiating graceful shutdown...`);
        await this.stop();
      });
    });

    process.on('uncaughtException', async (error) => {
      logger.error('Uncaught exception in consumer', { 
        error: error.message,
        stack: error.stack
      });
      await this.stop();
    });

    process.on('unhandledRejection', async (reason, promise) => {
      logger.error('Unhandled promise rejection in consumer', { 
        reason: reason?.message || reason,
        promise: promise.toString()
      });
      await this.stop();
    });
  }
}

// Create consumer manager
const consumerManager = new ConsumerManager();

// Handle special modes before starting main consumer
async function startConsumer() {
  try {
    // Handle special modes (reset offsets, seek, etc.)
    if (consumerManager.options.resetOffsets || 
        consumerManager.options.seekToEnd || 
        consumerManager.options.seekToBeginning) {
      await consumerManager.handleSpecialModes();
    }

    // Setup graceful shutdown
    consumerManager.setupGracefulShutdown();

    // Start the consumer
    await consumerManager.start();
    
  } catch (error) {
    logger.error('Consumer startup failed', { 
      error: error.message,
      stack: error.stack
    });
    process.exit(1);
  }
}

// Start the consumer
startConsumer();