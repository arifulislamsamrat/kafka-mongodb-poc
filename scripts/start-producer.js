#!/usr/bin/env node

const KafkaProducerService = require('../src/services/kafkaProducer');
const config = require('../src/config');
const logger = require('../src/utils/logger');

class TestProducer {
  constructor() {
    this.producer = new KafkaProducerService();
    this.isRunning = false;
    this.messageCount = 0;
    this.errorCount = 0;
    this.interval = null;
    this.batchInterval = null;
    this.stats = {
      startTime: Date.now(),
      messagesPerSecond: 0,
      averageLatency: 0,
      totalLatency: 0
    };
  }

  async start() {
    try {
      logger.info('Starting test producer...');
      
      // Parse command line arguments
      const options = this.parseCommandLineArgs();
      this.options = options;
      
      logger.info('Producer options', options);
      
      await this.producer.connect();
      this.isRunning = true;
      
      // Start appropriate sending mode
      if (options.batchMode) {
        await this.startBatchMode();
      } else {
        await this.startSingleMessageMode();
      }

      // Start statistics reporting
      this.startStatsReporting();

      logger.info('Test producer started successfully');
      
      // Auto-stop after specified duration
      if (options.duration > 0) {
        setTimeout(() => {
          this.stop();
        }, options.duration * 1000);
      }

    } catch (error) {
      logger.error('Failed to start test producer', { 
        error: error.message 
      });
      throw error;
    }
  }

  parseCommandLineArgs() {
    const args = process.argv.slice(2);
    const options = {
      messageRate: 1, // messages per second
      duration: 60, // seconds (0 = infinite)
      batchMode: false,
      batchSize: 10,
      topic: config.kafka.topic,
      userCount: 1000,
      messageType: 'mixed', // 'mixed', 'login', 'purchase', etc.
      burst: false,
      burstCount: 100,
      burstInterval: 30 // seconds
    };

    for (let i = 0; i < args.length; i++) {
      switch (args[i]) {
        case '--rate':
        case '-r':
          options.messageRate = parseInt(args[i + 1]) || 1;
          i++;
          break;
        case '--duration':
        case '-d':
          options.duration = parseInt(args[i + 1]) || 60;
          i++;
          break;
        case '--batch':
        case '-b':
          options.batchMode = true;
          break;
        case '--batch-size':
          options.batchSize = parseInt(args[i + 1]) || 10;
          i++;
          break;
        case '--topic':
        case '-t':
          options.topic = args[i + 1] || config.kafka.topic;
          i++;
          break;
        case '--users':
        case '-u':
          options.userCount = parseInt(args[i + 1]) || 1000;
          i++;
          break;
        case '--type':
          options.messageType = args[i + 1] || 'mixed';
          i++;
          break;
        case '--burst':
          options.burst = true;
          break;
        case '--burst-count':
          options.burstCount = parseInt(args[i + 1]) || 100;
          i++;
          break;
        case '--burst-interval':
          options.burstInterval = parseInt(args[i + 1]) || 30;
          i++;
          break;
        case '--help':
        case '-h':
          this.printUsage();
          process.exit(0);
          break;
      }
    }

    return options;
  }

  printUsage() {
    console.log(`
Usage: node scripts/start-producer.js [OPTIONS]

Options:
  -r, --rate <number>         Messages per second (default: 1)
  -d, --duration <seconds>    Duration in seconds (0 = infinite, default: 60)
  -b, --batch                 Enable batch mode
  --batch-size <number>       Batch size (default: 10)
  -t, --topic <string>        Kafka topic (default: from config)
  -u, --users <number>        Number of unique users (default: 1000)
  --type <string>             Message type: mixed|login|purchase|view_product (default: mixed)
  --burst                     Enable burst mode
  --burst-count <number>      Messages per burst (default: 100)
  --burst-interval <seconds>  Seconds between bursts (default: 30)
  -h, --help                  Show this help

Examples:
  # Send 10 messages per second for 30 seconds
  node scripts/start-producer.js --rate 10 --duration 30
  
  # Send batches of 50 messages every 5 seconds
  node scripts/start-producer.js --batch --batch-size 50 --rate 0.2
  
  # Send only login events for 100 users
  node scripts/start-producer.js --type login --users 100
  
  # Burst mode: 500 messages every 60 seconds
  node scripts/start-producer.js --burst --burst-count 500 --burst-interval 60
    `);
  }

  async startSingleMessageMode() {
    const intervalMs = this.options.messageRate > 0 ? 1000 / this.options.messageRate : 5000;
    
    // Send initial message
    await this.sendTestMessage();
    
    // Setup interval for continuous sending
    this.interval = setInterval(async () => {
      if (this.isRunning) {
        try {
          if (this.options.burst) {
            await this.sendBurst();
          } else {
            await this.sendTestMessage();
          }
        } catch (error) {
          this.errorCount++;
          logger.error('Failed to send test message', { 
            error: error.message 
          });
        }
      }
    }, intervalMs);
  }

  async startBatchMode() {
    const intervalMs = this.options.messageRate > 0 ? 1000 / this.options.messageRate : 5000;
    
    // Send initial batch
    await this.sendTestBatch();
    
    // Setup interval for batch sending
    this.batchInterval = setInterval(async () => {
      if (this.isRunning) {
        try {
          await this.sendTestBatch();
        } catch (error) {
          this.errorCount++;
          logger.error('Failed to send test batch', { 
            error: error.message 
          });
        }
      }
    }, intervalMs);
  }

  async sendBurst() {
    logger.info('Sending message burst', { 
      burstCount: this.options.burstCount 
    });
    
    const messages = [];
    for (let i = 0; i < this.options.burstCount; i++) {
      messages.push(this.generateTestEvent());
    }
    
    try {
      const startTime = Date.now();
      const result = await this.producer.sendBatch(this.options.topic, messages, {
        batchId: `burst-${Date.now()}`,
        messageId: `burst-${this.messageCount}`
      });
      
      const latency = Date.now() - startTime;
      this.updateStats(latency, messages.length);
      
      this.messageCount += messages.length;
      
      logger.info('Message burst sent', {
        burstSize: messages.length,
        totalMessages: this.messageCount,
        latency: `${latency}ms`,
        partitions: result.map(r => r.partition),
        baseOffsets: result.map(r => r.baseOffset)
      });
      
    } catch (error) {
      this.errorCount += this.options.burstCount;
      throw error;
    }
  }

  async sendTestBatch() {
    const messages = [];
    for (let i = 0; i < this.options.batchSize; i++) {
      messages.push(this.generateTestEvent());
    }
    
    try {
      const startTime = Date.now();
      const result = await this.producer.sendBatch(this.options.topic, messages, {
        batchId: `batch-${Date.now()}`,
        messageId: `batch-${this.messageCount}`
      });
      
      const latency = Date.now() - startTime;
      this.updateStats(latency, messages.length);
      
      this.messageCount += messages.length;
      
      logger.info('Test batch sent', {
        batchSize: messages.length,
        totalMessages: this.messageCount,
        latency: `${latency}ms`,
        partitions: result.map(r => r.partition),
        baseOffsets: result.map(r => r.baseOffset)
      });
      
    } catch (error) {
      this.errorCount += this.options.batchSize;
      throw error;
    }
  }

  async sendTestMessage() {
    try {
      const testEvent = this.generateTestEvent();
      
      const startTime = Date.now();
      const result = await this.producer.sendUserEvent(testEvent, {
        topic: this.options.topic,
        messageId: `test-${Date.now()}-${this.messageCount}`,
        correlationId: `correlation-${Date.now()}`
      });
      
      const latency = Date.now() - startTime;
      this.updateStats(latency, 1);
      
      this.messageCount++;

      logger.debug('Test message sent', {
        messageCount: this.messageCount,
        userId: testEvent.userId,
        action: testEvent.action,
        partition: result.partition,
        offset: result.baseOffset,
        latency: `${latency}ms`
      });

    } catch (error) {
      this.errorCount++;
      logger.error('Failed to send test message', { 
        error: error.message 
      });
      throw error;
    }
  }

  generateTestEvent() {
    let action;
    
    if (this.options.messageType === 'mixed') {
      const actions = ['login', 'logout', 'purchase', 'view_product', 'add_to_cart', 'remove_from_cart', 'search', 'signup'];
      action = actions[Math.floor(Math.random() * actions.length)];
    } else {
      action = this.options.messageType;
    }
    
    const browsers = ['Chrome', 'Firefox', 'Safari', 'Edge', 'Opera'];
    const currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD'];
    const countries = ['US', 'GB', 'DE', 'FR', 'CA', 'AU', 'JP'];
    const devices = ['desktop', 'mobile', 'tablet'];
    
    const userId = Math.floor(Math.random() * this.options.userCount) + 1;
    
    const baseEvent = {
      userId,
      action,
      timestamp: new Date().toISOString(),
      metadata: {
        browser: browsers[Math.floor(Math.random() * browsers.length)],
        ip: this.generateRandomIP(),
        sessionId: `session_${Math.random().toString(36).substr(2, 9)}`,
        userAgent: this.generateUserAgent(),
        country: countries[Math.floor(Math.random() * countries.length)],
        device: devices[Math.floor(Math.random() * devices.length)],
        referrer: Math.random() > 0.3 ? this.generateReferrer() : null
      }
    };

    // Add action-specific metadata
    switch (action) {
      case 'purchase':
        baseEvent.metadata.amount = Math.floor(Math.random() * 1000) + 10;
        baseEvent.metadata.currency = currencies[Math.floor(Math.random() * currencies.length)];
        baseEvent.metadata.productId = `product_${Math.floor(Math.random() * 500) + 1}`;
        baseEvent.metadata.quantity = Math.floor(Math.random() * 5) + 1;
        baseEvent.metadata.category = this.generateProductCategory();
        break;
      
      case 'view_product':
      case 'add_to_cart':
      case 'remove_from_cart':
        baseEvent.metadata.productId = `product_${Math.floor(Math.random() * 500) + 1}`;
        baseEvent.metadata.category = this.generateProductCategory();
        baseEvent.metadata.price = Math.floor(Math.random() * 500) + 10;
        if (action === 'add_to_cart' || action === 'remove_from_cart') {
          baseEvent.metadata.quantity = Math.floor(Math.random() * 5) + 1;
        }
        break;
      
      case 'search':
        const searchTerms = [
          'laptop', 'phone', 'headphones', 'shoes', 'book', 'camera',
          'watch', 'keyboard', 'mouse', 'monitor', 'tablet', 'speaker'
        ];
        baseEvent.metadata.searchQuery = searchTerms[Math.floor(Math.random() * searchTerms.length)];
        baseEvent.metadata.resultsCount = Math.floor(Math.random() * 100) + 1;
        break;
        
      case 'signup':
        baseEvent.metadata.signupMethod = Math.random() > 0.5 ? 'email' : 'social';
        baseEvent.metadata.newsletter = Math.random() > 0.3;
        break;
        
      case 'login':
        baseEvent.metadata.loginMethod = Math.random() > 0.7 ? 'social' : 'email';
        baseEvent.metadata.rememberMe = Math.random() > 0.5;
        break;
    }

    return baseEvent;
  }

  generateProductCategory() {
    const categories = [
      'Electronics', 'Clothing', 'Books', 'Home & Garden', 
      'Sports', 'Beauty', 'Toys', 'Automotive', 'Health'
    ];
    return categories[Math.floor(Math.random() * categories.length)];
  }

  generateReferrer() {
    const referrers = [
      'https://google.com',
      'https://facebook.com',
      'https://twitter.com',
      'https://instagram.com',
      'https://youtube.com',
      'direct'
    ];
    return referrers[Math.floor(Math.random() * referrers.length)];
  }

  generateRandomIP() {
    // Generate realistic IP ranges
    const ranges = [
      [192, 168], // Private
      [10, 0],    // Private
      [172, 16],  // Private
      [203, 0],   // Public
      [198, 51],  // Public
      [151, 101]  // Public
    ];
    
    const range = ranges[Math.floor(Math.random() * ranges.length)];
    return [
      range[0],
      range[1],
      Math.floor(Math.random() * 255),
      Math.floor(Math.random() * 255)
    ].join('.');
  }

  generateUserAgent() {
    const userAgents = [
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
      'Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1'
    ];
    
    return userAgents[Math.floor(Math.random() * userAgents.length)];
  }

  updateStats(latency, messageCount) {
    this.stats.totalLatency += latency;
    this.stats.averageLatency = this.stats.totalLatency / this.messageCount;
    
    const elapsedSeconds = (Date.now() - this.stats.startTime) / 1000;
    this.stats.messagesPerSecond = this.messageCount / elapsedSeconds;
  }

  startStatsReporting() {
    setInterval(() => {
      if (this.isRunning) {
        const uptime = Math.floor((Date.now() - this.stats.startTime) / 1000);
        logger.info('Producer Statistics', {
          uptime: `${uptime}s`,
          messagesProduced: this.messageCount,
          errors: this.errorCount,
          messagesPerSecond: this.stats.messagesPerSecond.toFixed(2),
          averageLatency: `${this.stats.averageLatency.toFixed(2)}ms`,
          successRate: `${((this.messageCount / (this.messageCount + this.errorCount)) * 100).toFixed(2)}%`
        });
      }
    }, 10000); // Report every 10 seconds
  }

  async stop() {
    try {
      logger.info('Stopping test producer...');
      this.isRunning = false;
      
      if (this.interval) {
        clearInterval(this.interval);
        this.interval = null;
      }
      
      if (this.batchInterval) {
        clearInterval(this.batchInterval);
        this.batchInterval = null;
      }
      
      await this.producer.disconnect();
      
      // Final statistics
      const totalTime = (Date.now() - this.stats.startTime) / 1000;
      logger.info('Test producer stopped - Final Statistics', { 
        totalMessagesSent: this.messageCount,
        totalErrors: this.errorCount,
        totalTime: `${totalTime.toFixed(2)}s`,
        averageRate: `${(this.messageCount / totalTime).toFixed(2)} msg/s`,
        averageLatency: `${this.stats.averageLatency.toFixed(2)}ms`,
        successRate: `${((this.messageCount / (this.messageCount + this.errorCount)) * 100).toFixed(2)}%`
      });
      
      process.exit(0);
    } catch (error) {
      logger.error('Error stopping test producer', { 
        error: error.message 
      });
      process.exit(1);
    }
  }

  setupGracefulShutdown() {
    const signals = ['SIGINT', 'SIGTERM', 'SIGQUIT'];
    
    signals.forEach(signal => {
      process.on(signal, async () => {
        logger.info(`Received ${signal}, stopping producer...`);
        await this.stop();
      });
    });

    process.on('uncaughtException', async (error) => {
      logger.error('Uncaught exception in producer', { 
        error: error.message,
        stack: error.stack
      });
      await this.stop();
    });

    process.on('unhandledRejection', async (reason, promise) => {
      logger.error('Unhandled promise rejection in producer', { 
        reason: reason?.message || reason,
        promise: promise.toString()
      });
      await this.stop();
    });
  }
}

// Create and start producer
const producer = new TestProducer();

// Setup graceful shutdown
producer.setupGracefulShutdown();

// Start the producer
producer.start().catch(error => {
  logger.error('Test producer startup failed', { 
    error: error.message,
    stack: error.stack
  });
  process.exit(1);
});