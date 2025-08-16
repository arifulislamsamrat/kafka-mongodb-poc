const kafkaConfig = require('../config/kafka');
const config = require('../config');
const logger = require('../utils/logger');
const errorHandler = require('../utils/errorHandler');
const validator = require('../utils/validator');

class KafkaConsumerService {
  constructor() {
    this.consumer = null;
    this.isRunning = false;
    this.messageCount = 0;
    this.errorCount = 0;
    this.lastMessageTime = null;
    this.messageHandlers = new Map();
    this.circuitBreaker = errorHandler.createCircuitBreaker('kafka-consumer');
  }

  async initialize() {
    try {
      logger.kafkaLog('Initializing consumer');
      this.consumer = kafkaConfig.getConsumer();
      
      // Set up event listeners
      this.setupEventListeners();
      
      logger.kafkaLog('Consumer initialized successfully');
    } catch (error) {
      logger.error('Failed to initialize Kafka consumer', { 
        error: error.message 
      });
      throw error;
    }
  }

  setupEventListeners() {
    if (!this.consumer) return;

    this.consumer.on('consumer.connect', () => {
      logger.kafkaLog('Consumer connected');
    });

    this.consumer.on('consumer.disconnect', () => {
      logger.kafkaLog('Consumer disconnected');
      this.isRunning = false;
    });

    this.consumer.on('consumer.stop', () => {
      logger.kafkaLog('Consumer stopped');
      this.isRunning = false;
    });

    this.consumer.on('consumer.crash', ({ error }) => {
      logger.error('Consumer crashed', { error: error.message });
      this.errorCount++;
      this.isRunning = false;
    });

    this.consumer.on('consumer.rebalancing', () => {
      logger.kafkaLog('Consumer rebalancing');
    });

    this.consumer.on('consumer.group_join', ({ duration }) => {
      logger.kafkaLog('Consumer joined group', { duration });
    });

    this.consumer.on('consumer.fetch', ({ numberOfBatches, duration }) => {
      logger.debug('Consumer fetch completed', { 
        numberOfBatches, 
        duration 
      });
    });

    this.consumer.on('consumer.start_batch_process', ({ topic, partition, firstOffset, lastOffset }) => {
      logger.debug('Starting batch process', {
        topic,
        partition,
        firstOffset,
        lastOffset,
        batchSize: parseInt(lastOffset) - parseInt(firstOffset) + 1
      });
    });

    this.consumer.on('consumer.end_batch_process', ({ topic, partition, firstOffset, lastOffset, duration }) => {
      logger.debug('Batch process completed', {
        topic,
        partition,
        firstOffset,
        lastOffset,
        duration
      });
    });
  }

  async connect() {
    try {
      if (!this.consumer) {
        await this.initialize();
      }

      logger.kafkaLog('Connecting consumer');
      await this.consumer.connect();
      logger.kafkaLog('Consumer connected successfully');
    } catch (error) {
      const retryInfo = errorHandler.handleKafkaError(error, { 
        operation: 'connect' 
      });
      
      if (retryInfo.shouldRetry) {
        logger.warn('Retrying consumer connection', { 
          delay: retryInfo.delay 
        });
        await errorHandler.sleep(retryInfo.delay);
        return this.connect();
      }
      
      throw error;
    }
  }

  async disconnect() {
    try {
      if (this.consumer && this.isRunning) {
        logger.kafkaLog('Disconnecting consumer');
        this.isRunning = false;
        await this.consumer.disconnect();
        logger.kafkaLog('Consumer disconnected successfully');
      }
    } catch (error) {
      logger.error('Error disconnecting consumer', { 
        error: error.message 
      });
      throw error;
    }
  }

  async subscribe(topics = [config.kafka.topic]) {
    try {
      logger.kafkaLog('Subscribing to topics', { topics });
      
      await this.consumer.subscribe({ 
        topics,
        fromBeginning: false 
      });
      
      logger.kafkaLog('Successfully subscribed to topics', { topics });
    } catch (error) {
      logger.error('Failed to subscribe to topics', {
        topics,
        error: error.message
      });
      throw error;
    }
  }

  registerMessageHandler(messageType, handler) {
    this.messageHandlers.set(messageType, handler);
    logger.appLog('Registered message handler', { messageType });
  }

  async processMessage(message, topic, partition) {
    try {
      // Validate the message structure
      const validatedMessage = validator.validateKafkaMessage({
        topic,
        partition,
        offset: message.offset,
        timestamp: message.timestamp,
        key: message.key,
        value: message.value,
        headers: message.headers
      });

      // Check if message should be processed
      if (!validator.shouldProcessMessage(validatedMessage)) {
        logger.debug('Message filtered out', {
          messageId: validator.generateMessageId(validatedMessage)
        });
        return;
      }

      // Enrich the message
      const enrichedMessage = validator.enrichMessage(validatedMessage);

      // Determine message type and get appropriate handler
      const messageType = this.getMessageType(enrichedMessage);
      const handler = this.messageHandlers.get(messageType) || 
                    this.messageHandlers.get('default');

      if (!handler) {
        throw new Error(`No handler found for message type: ${messageType}`);
      }

      // Process the message
      await handler(enrichedMessage);

      // Update metrics
      this.messageCount++;
      this.lastMessageTime = new Date();

      logger.debug('Message processed successfully', {
        messageId: enrichedMessage.id,
        messageType,
        offset: message.offset,
        partition
      });

    } catch (error) {
      this.errorCount++;
      
      const retryInfo = errorHandler.handleProcessingError(error, message, {
        topic,
        partition,
        messageType: this.getMessageType(message)
      });

      if (retryInfo.shouldRetry) {
        logger.warn('Retrying message processing', {
          messageId: validator.generateMessageId({ topic, partition, offset: message.offset }),
          attempt: retryInfo.attempt,
          delay: retryInfo.delay
        });
        
        await errorHandler.sleep(retryInfo.delay);
        return this.processMessage(message, topic, partition);
      }

      // If we can't retry, log and continue
      logger.error('Failed to process message after retries', {
        messageId: validator.generateMessageId({ topic, partition, offset: message.offset }),
        error: error.message
      });
    }
  }

  getMessageType(message) {
    // Extract message type from headers or value
    if (message.headers && message.headers.messageType) {
      return message.headers.messageType.toString();
    }

    if (message.value && typeof message.value === 'object' && message.value.type) {
      return message.value.type;
    }

    if (message.value && typeof message.value === 'object' && message.value.action) {
      return `user_${message.value.action}`;
    }

    return 'default';
  }

  async startConsuming() {
    try {
      if (this.isRunning) {
        logger.warn('Consumer is already running');
        return;
      }

      logger.kafkaLog('Starting message consumption');
      this.isRunning = true;

      await this.circuitBreaker.execute(async () => {
        await this.consumer.run({
          partitionsConsumedConcurrently: config.kafka.consumer.partitionConcurrency,
          eachMessage: async ({ topic, partition, message }) => {
            if (!this.isRunning) {
              logger.debug('Skipping message processing - consumer is stopping');
              return;
            }

            try {
              await this.processMessage(message, topic, partition);
            } catch (error) {
              logger.error('Error in message processing', {
                topic,
                partition,
                offset: message.offset,
                error: error.message
              });
              // Don't rethrow here to avoid stopping the consumer
            }
          },
          eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
            if (!this.isRunning || !isRunning() || isStale()) {
              return;
            }

            logger.debug('Processing batch', {
              topic: batch.topic,
              partition: batch.partition,
              messagesCount: batch.messages.length,
              firstOffset: batch.firstOffset(),
              lastOffset: batch.lastOffset()
            });

            for (const message of batch.messages) {
              if (!this.isRunning || !isRunning() || isStale()) {
                break;
              }

              try {
                await this.processMessage(message, batch.topic, batch.partition);
                resolveOffset(message.offset);
                await heartbeat();
              } catch (error) {
                logger.error('Error processing message in batch', {
                  topic: batch.topic,
                  partition: batch.partition,
                  offset: message.offset,
                  error: error.message
                });
                // Continue processing other messages in the batch
              }
            }
          }
        });
      });

      logger.kafkaLog('Consumer started successfully');
    } catch (error) {
      this.isRunning = false;
      const retryInfo = errorHandler.handleKafkaError(error, { 
        operation: 'startConsuming' 
      });
      
      if (retryInfo.shouldRetry) {
        logger.warn('Retrying consumer start', { 
          delay: retryInfo.delay 
        });
        await errorHandler.sleep(retryInfo.delay);
        return this.startConsuming();
      }
      
      throw error;
    }
  }

  async stop() {
    try {
      logger.kafkaLog('Stopping consumer');
      this.isRunning = false;
      
      if (this.consumer) {
        await this.consumer.stop();
        logger.kafkaLog('Consumer stopped successfully');
      }
    } catch (error) {
      logger.error('Error stopping consumer', { 
        error: error.message 
      });
      throw error;
    }
  }

  async seek(topic, partition, offset) {
    try {
      logger.kafkaLog('Seeking to offset', { topic, partition, offset });
      await this.consumer.seek({ topic, partition, offset });
      logger.kafkaLog('Seek completed', { topic, partition, offset });
    } catch (error) {
      logger.error('Failed to seek', {
        topic,
        partition,
        offset,
        error: error.message
      });
      throw error;
    }
  }

  async pause(topics) {
    try {
      logger.kafkaLog('Pausing topics', { topics });
      this.consumer.pause(topics);
      logger.kafkaLog('Topics paused', { topics });
    } catch (error) {
      logger.error('Failed to pause topics', {
        topics,
        error: error.message
      });
      throw error;
    }
  }

  async resume(topics) {
    try {
      logger.kafkaLog('Resuming topics', { topics });
      this.consumer.resume(topics);
      logger.kafkaLog('Topics resumed', { topics });
    } catch (error) {
      logger.error('Failed to resume topics', {
        topics,
        error: error.message
      });
      throw error;
    }
  }

  getMetrics() {
    return {
      isRunning: this.isRunning,
      messageCount: this.messageCount,
      errorCount: this.errorCount,
      lastMessageTime: this.lastMessageTime,
      circuitBreakerState: this.circuitBreaker.getState(),
      successRate: this.messageCount > 0 ? 
        ((this.messageCount - this.errorCount) / this.messageCount * 100).toFixed(2) : 0
    };
  }

  async healthCheck() {
    try {
      const metrics = this.getMetrics();
      const kafkaHealth = await kafkaConfig.healthCheck();
      
      return {
        status: this.isRunning && kafkaHealth.status === 'healthy' ? 'healthy' : 'unhealthy',
        consumer: {
          running: this.isRunning,
          messageCount: this.messageCount,
          errorCount: this.errorCount,
          lastMessageTime: this.lastMessageTime,
          successRate: metrics.successRate
        },
        kafka: kafkaHealth,
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

  async commitOffsets(topics) {
    try {
      if (this.consumer) {
        await this.consumer.commitOffsets(topics);
        logger.debug('Offsets committed', { topics });
      }
    } catch (error) {
      logger.error('Failed to commit offsets', {
        topics,
        error: error.message
      });
      throw error;
    }
  }

  async resetOffsets(groupId, topic) {
    try {
      const admin = kafkaConfig.getAdmin();
      await admin.connect();
      
      await admin.resetOffsets({
        groupId,
        topic,
        earliest: true
      });
      
      await admin.disconnect();
      logger.kafkaLog('Offsets reset', { groupId, topic });
    } catch (error) {
      logger.error('Failed to reset offsets', {
        groupId,
        topic,
        error: error.message
      });
      throw error;
    }
  }
}

module.exports = KafkaConsumerService;}