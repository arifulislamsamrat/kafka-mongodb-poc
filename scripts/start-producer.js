#!/usr/bin/env node

const KafkaProducerService = require('../src/services/kafkaProducer');
const config = require('../src/config');
const logger = require('../src/utils/logger');

class TestProducer {
  constructor() {
    this.producer = new KafkaProducerService();
    this.isRunning = false;
    this.messageCount = 0;
    this.interval = null;
  }

  async start() {
    try {
      logger.info('Starting test producer...');
      await this.producer.connect();
      
      this.isRunning = true;
      
      // Send initial message
      await this.sendTestMessage();
      
      // Send messages every 5 seconds
      this.interval = setInterval(async () => {
        if (this.isRunning) {
          try {
            await this.sendTestMessage();
          } catch (error) {
            logger.error('Failed to send test message', { 
              error: error.message 
            });
          }
        }
      }, 5000);

      logger.info('Test producer started successfully');
      
      // Stop after 60 seconds
      setTimeout(() => {
        this.stop();
      }, 60000);

    } catch (error) {
      logger.error('Failed to start test producer', { 
        error: error.message 
      });
      throw error;
    }
  }

  async stop() {
    try {
      logger.info('Stopping test producer...');
      this.isRunning = false;
      
      if (this.interval) {
        clearInterval(this.interval);
        this.interval = null;
      }
      
      await this.producer.disconnect();
      
      logger.info('Test producer stopped', { 
        totalMessagesSent: this.messageCount 
      });
      
      process.exit(0);
    } catch (error) {
      logger.error('Error stopping test producer', { 
        error: error.message 
      });
      process.exit(1);
    }
  }

  async sendTestMessage() {
    try {
      const testEvent = this.generateTestEvent();
      
      const result = await this.producer.sendUserEvent(testEvent, {
        topic: config.kafka.topic,
        messageId: `test-${Date.now()}-${this.messageCount}`,
        correlationId: `correlation-${Date.now()}`
      });

      this.messageCount++;

      logger.info('Test message sent', {
        messageCount: this.messageCount,
        userId: testEvent.userId,
        action: testEvent.action,
        partition: result.partition,
        offset: result.baseOffset
      });

    } catch (error) {
      logger.error('Failed to send test message', { 
        error: error.message 
      });
      throw error;
    }
  }

  generateTestEvent() {
    const actions = ['login', 'logout', 'purchase', 'view_product', 'add_to_cart', 'remove_from_cart', 'search'];
    const browsers = ['Chrome', 'Firefox', 'Safari', 'Edge'];
    const currencies = ['USD', 'EUR', 'GBP', 'JPY'];
    
    const action = actions[Math.floor(Math.random() * actions.length)];
    const userId = Math.floor(Math.random() * 1000) + 1;
    
    const baseEvent = {
      userId,
      action,
      timestamp: new Date().toISOString(),
      metadata: {
        browser: browsers[Math.floor(Math.random() * browsers.length)],
        ip: this.generateRandomIP(),
        sessionId: `session_${Math.random().toString(36).substr(2, 9)}`,
        userAgent: this.generateUserAgent()
      }
    };

    // Add action-specific metadata
    s