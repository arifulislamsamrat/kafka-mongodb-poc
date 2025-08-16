const kafkaConfig = require('../config/kafka');
const config = require('../config');
const logger = require('../utils/logger');
const errorHandler = require('../utils/errorHandler');
const validator = require('../utils/validator');
const { v4: uuidv4 } = require('crypto').randomUUID || require('uuid').v4;

class KafkaProducerService {
  constructor() {
    this.producer = null;
    this.isConnected = false;
    this.messageCount = 0;
    this.errorCount = 0;
    this.circuitBreaker = errorHandler.createCircuitBreaker('kafka-producer');
  }

  async initialize() {
    try {
      logger.kafkaLog('Initializing producer');
      this.producer = kafkaConfig.getProducer();
      
      // Set up event listeners
      this.setupEventListeners();
      
      logger.kafkaLog('Producer initialized successfully');
    } catch (error) {
      logger.error('Failed to initialize Kafka producer', { 
        error: error.message 
      });
      throw error;
    }
  }

  setupEventListeners() {
    if (!this.producer) return;

    this.producer.on('producer.connect', () => {
      logger.kafkaLog('Producer connected');
      this.isConnected = true;
    });

    this.producer.on('producer.disconnect', () => {
      logger.kafkaLog('Producer disconnected');
      this.isConnected = false;
    });

    this.producer.on('producer.network.request_timeout', ({ broker, clientId, createdAt, sentAt }) => {
      logger.warn('Producer request timeout', {
        broker,
        clientId,
        duration: Date.now() - createdAt
      });
    });
  }

  async connect() {
    try {
      if (!this.producer) {
        await this.initialize();
      }

      if (this.isConnected) {
        logger.debug('Producer already connected');
        return;
      }

      logger.kafkaLog('Connecting producer');
      await this.producer.connect();
      this.isConnected = true;
      logger.kafkaLog('Producer connected successfully');
    } catch (error) {
      const retryInfo = errorHandler.handleKafkaError(error, { 
        operation: 'connect' 
      });
      
      if (retryInfo.shouldRetry) {
        logger.warn('Retrying producer connection', { 
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
      if (this.producer && this.isConnected) {
        logger.kafkaLog('Disconnecting producer');
        await this.producer.disconnect();
        this.isConnected = false;
        logger.kafkaLog('Producer disconnected successfully');
      }
    } catch (error) {
      logger.error('Error disconnecting producer', { 
        error: error.message 
      });
      throw error;
    }
  }

  async sendMessage(topic, message, options = {}) {
    try {
      if (!this.isConnected) {
        await this.connect();
      }

      // Prepare message
      const kafkaMessage = this.prepareMessage(message, options);
      
      // Validate message
      validator.validateKafkaMessage({
        topic,
        partition: kafkaMessage.partition,
        offset: '0', // Will be set by Kafka
        timestamp: Date.now(),
        key: kafkaMessage.key,
        value: kafkaMessage.value,
        headers: kafkaMessage.headers
      });

      const result = await this.circuitBreaker.execute(async () => {
        return await this.producer.send({
          topic,
          messages: [kafkaMessage],
          acks: options.acks || -1, // Wait for all replicas
          timeout: options.timeout || 30000,
          compression: options.compression || 'gzip'
        });
      });

      this.messageCount++;
      
      logger.debug('Message sent successfully', {
        topic,
        partition: result[0].partition,
        offset: result[0].baseOffset,
        messageId: options.messageId || 'unknown'
      });

      return result[0];
    } catch (error) {
      this.errorCount++;
      
      const retryInfo = errorHandler.handleKafkaError(error, {
        operation: 'sendMessage',
        topic,
        messageId: options.messageId
      });

      if (retryInfo.shouldRetry) {
        logger.warn('Retrying message send', {
          topic,
          messageId: options.messageId,
          attempt: retryInfo.attempt,
          delay: retryInfo.delay
        });
        
        await errorHandler.sleep(retryInfo.delay);
        return this.sendMessage(topic, message, {
          ...options,
          attempt: retryInfo.attempt
        });
      }

      logger.error('Failed to send message', {
        topic,
        messageId: options.messageId,
        error: error.message
      });
      throw error;
    }
  }

  async sendBatch(topic, messages, options = {}) {
    try {
      if (!this.isConnected) {
        await this.connect();
      }

      // Prepare all messages
      const kafkaMessages = messages.map((message, index) => 
        this.prepareMessage(message, {
          ...options,
          messageId: options.messageId ? `${options.messageId}-${index}` : undefined
        })
      );

      // Validate batch
      validator.validateBatch({
        messages: kafkaMessages,
        batchId: options.batchId || uuidv4(),
        createdAt: new Date(),
        size: kafkaMessages.length
      });

      const result = await this.circuitBreaker.execute(async () => {
        return await this.producer.send({
          topic,
          messages: kafkaMessages,
          acks: options.acks || -1,
          timeout: options.timeout || 30000,
          compression: options.compression || 'gzip'
        });
      });

      this.messageCount += kafkaMessages.length;

      logger.info('Batch sent successfully', {
        topic,
        batchSize: kafkaMessages.length,
        batchId: options.batchId,
        partitions: result.map(r => r.partition),
        baseOffsets: result.map(r => r.baseOffset)
      });

      return result;
    } catch (error) {
      this.errorCount += messages.length;
      
      const retryInfo = errorHandler.handleKafkaError(error, {
        operation: 'sendBatch',
        topic,
        batchSize: messages.length,
        batchId: options.batchId
      });

      if (retryInfo.shouldRetry) {
        logger.warn('Retrying batch send', {
          topic,
          batchSize: messages.length,
          batchId: options.batchId,
          attempt: retryInfo.attempt,
          delay: retryInfo.delay
        });
        
        await errorHandler.sleep(retryInfo.delay);
        return this.sendBatch(topic, messages, {
          ...options,
          attempt: retryInfo.attempt
        });
      }

      logger.error('Failed to send batch', {
        topic,
        batchSize: messages.length,
        batchId: options.batchId,
        error: error.message
      });
      throw error;
    }
  }

  prepareMessage(message, options = {}) {
    const kafkaMessage = {
      key: options.key || null,
      value: this.serializeValue(message),
      headers: this.prepareHeaders(options.headers),
      timestamp: options.timestamp || Date.now().toString()
    };

    // Add partition if specified
    if (options.partition !== undefined) {
      kafkaMessage.partition = options.partition;
    }

    // Add message ID to headers if provided
    if (options.messageId) {
      kafkaMessage.headers = kafkaMessage.headers || {};
      kafkaMessage.headers.messageId = options.messageId;
    }

    // Add correlation ID for tracing
    if (options.correlationId) {
      kafkaMessage.headers = kafkaMessage.headers || {};
      kafkaMessage.headers.correlationId = options.correlationId;
    }

    // Add message type for routing
    if (options.messageType) {
      kafkaMessage.headers = kafkaMessage.headers || {};
      kafkaMessage.headers.messageType = options.messageType;
    }

    return kafkaMessage;
  }

  serializeValue(value) {
    if (typeof value === 'string') {
      return value;
    }
    
    if (Buffer.isBuffer(value)) {
      return value;
    }
    
    try {
      return JSON.stringify(value);
    } catch (error) {
      logger.error('Failed to serialize message value', { 
        error: error.message,
        valueType: typeof value
      });
      throw new Error('Unable to serialize message value');
    }
  }

  prepareHeaders(headers) {
    if (!headers) return null;
    
    const kafkaHeaders = {};
    
    for (const [key, value] of Object.entries(headers)) {
      if (value === null || value === undefined) {
        kafkaHeaders[key] = null;
      } else if (typeof value === 'string') {
        kafkaHeaders[key] = value;
      } else if (Buffer.isBuffer(value)) {
        kafkaHeaders[key] = value;
      } else {
        kafkaHeaders[key] = String(value);
      }
    }
    
    return kafkaHeaders;
  }

  async sendUserEvent(userEvent, options = {}) {
    try {
      // Validate user event
      const validatedEvent = validator.validateUserEvent(userEvent);
      
      // Generate message key based on userId
      const messageKey = `user_${validatedEvent.userId}`;
      
      // Prepare headers
      const headers = {
        messageType: 'user_event',
        action: validatedEvent.action,
        userId: validatedEvent.userId.toString(),
        version: '1.0',
        ...options.headers
      };

      return await this.sendMessage(
        options.topic || config.kafka.topic,
        validatedEvent,
        {
          key: messageKey,
          headers,
          messageType: 'user_event',
          ...options
        }
      );
    } catch (error) {
      logger.error('Failed to send user event', {
        error: error.message,
        userId: userEvent?.userId,
        action: userEvent?.action
      });
      throw error;
    }
  }

  async sendToDeadLetterQueue(originalMessage, error, options = {}) {
    try {
      const dlqMessage = {
        originalMessage: originalMessage,
        error: {
          message: error.message,
          stack: error.stack,
          timestamp: new Date().toISOString()
        },
        processingAttempts: options.attempts || 1,
        failureReason: options.reason || 'processing_error'
      };

      const headers = {
        messageType: 'dlq_message',
        originalTopic: originalMessage.topic,
        failureTimestamp: new Date().toISOString(),
        ...options.headers
      };

      return await this.sendMessage(
        config.errorHandling.dlqTopic,
        dlqMessage,
        {
          key: originalMessage.key,
          headers,
          messageType: 'dlq_message'
        }
      );
    } catch (dlqError) {
      logger.error('Failed to send message to DLQ', {
        originalError: error.message,
        dlqError: dlqError.message,
        messageKey: originalMessage.key
      });
      throw dlqError;
    }
  }

  getMetrics() {
    return {
      isConnected: this.isConnected,
      messageCount: this.messageCount,
      errorCount: this.errorCount,
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
        status: this.isConnected && kafkaHealth.status === 'healthy' ? 'healthy' : 'unhealthy',
        producer: {
          connected: this.isConnected,
          messageCount: this.messageCount,
          errorCount: this.errorCount,
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
}

module.exports = KafkaProducerService;