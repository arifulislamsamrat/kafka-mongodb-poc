const kafkaConfig = require('../config/kafka');
const config = require('../config');
const logger = require('../utils/logger');
const errorHandler = require('../utils/errorHandler');
const validator = require('../utils/validator');

class KafkaConsumerService {
  constructor() {
    this.consumer = null;
    this.isRunning = false;
    this.isPaused = false;
    this.messageCount = 0;
    this.errorCount = 0;
    this.lastMessageTime = null;
    this.startTime = Date.now();
    this.messageHandlers = new Map();
    this.circuitBreaker = errorHandler.createCircuitBreaker('kafka-consumer');
    this.processingStats = {
      totalProcessingTime: 0,
      averageProcessingTime: 0,
      maxProcessingTime: 0,
      minProcessingTime: Infinity
    };
    this.topicPartitionOffsets = new Map();
    this.consumerGroupMetadata = null;
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

    this.consumer.on('consumer.group_join', ({ duration, groupId, memberId, leaderId }) => {
      logger.kafkaLog('Consumer joined group', { 
        duration, 
        groupId, 
        memberId, 
        leaderId,
        isLeader: memberId === leaderId
      });
    });

    this.consumer.on('consumer.fetch', ({ numberOfBatches, duration }) => {
      logger.debug('Consumer fetch completed', { 
        numberOfBatches, 
        duration 
      });
    });

    this.consumer.on('consumer.start_batch_process', ({ topic, partition, firstOffset, lastOffset }) => {
      const batchSize = parseInt(lastOffset) - parseInt(firstOffset) + 1;
      logger.debug('Starting batch process', {
        topic,
        partition,
        firstOffset,
        lastOffset,
        batchSize
      });
    });

    this.consumer.on('consumer.end_batch_process', ({ topic, partition, firstOffset, lastOffset, duration }) => {
      const batchSize = parseInt(lastOffset) - parseInt(firstOffset) + 1;
      const avgTimePerMessage = duration / batchSize;
      
      logger.debug('Batch process completed', {
        topic,
        partition,
        firstOffset,
        lastOffset,
        duration,
        batchSize,
        avgTimePerMessage: `${avgTimePerMessage.toFixed(2)}ms`
      });
    });

    this.consumer.on('consumer.fetch_start', () => {
      logger.debug('Consumer fetch started');
    });

    this.consumer.on('consumer.network.request_timeout', ({ broker, clientId }) => {
      logger.warn('Consumer network request timeout', { broker, clientId });
    });

    this.consumer.on('consumer.received_unsubscribed_topics', ({ topics }) => {
      logger.warn('Received messages from unsubscribed topics', { topics });
    });
  }

  async connect() {
    try {
      if (!this.consumer) {
        await this.initialize();
      }

      logger.kafkaLog('Connecting consumer');
      await this.consumer.connect();
      
      // Get consumer group metadata
      this.consumerGroupMetadata = await this.getConsumerGroupMetadata();
      
      logger.kafkaLog('Consumer connected successfully', {
        groupId: config.kafka.groupId,
        clientId: config.kafka.clientId
      });
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
      if (!Array.isArray(topics)) {
        topics = [topics];
      }

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
    if (typeof handler !== 'function') {
      throw new Error('Handler must be a function');
    }

    this.messageHandlers.set(messageType, handler);
    logger.appLog('Registered message handler', { messageType });
  }

  async processMessage(message, topic, partition) {
    const startTime = Date.now();
    
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
        return { processed: false, reason: 'filtered' };
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
      const result = await handler(enrichedMessage);

      // Update statistics
      const processingTime = Date.now() - startTime;
      this.updateProcessingStats(processingTime);
      this.messageCount++;
      this.lastMessageTime = new Date();

      // Update topic-partition offset tracking
      this.updateOffsetTracking(topic, partition, message.offset);

      logger.debug('Message processed successfully', {
        messageId: enrichedMessage.id,
        messageType,
        offset: message.offset,
        partition,
        processingTime: `${processingTime}ms`
      });

      return { processed: true, result, processingTime };

    } catch (error) {
      const processingTime = Date.now() - startTime;
      this.errorCount++;
      
      const retryInfo = errorHandler.handleProcessingError(error, message, {
        topic,
        partition,
        messageType: this.getMessageType(message),
        processingTime
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
        error: error.message,
        processingTime: `${processingTime}ms`
      });

      return { processed: false, error: error.message, processingTime };
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

  updateProcessingStats(processingTime) {
    this.processingStats.totalProcessingTime += processingTime;
    this.processingStats.averageProcessingTime = 
      this.processingStats.totalProcessingTime / this.messageCount;
    
    if (processingTime > this.processingStats.maxProcessingTime) {
      this.processingStats.maxProcessingTime = processingTime;
    }
    
    if (processingTime < this.processingStats.minProcessingTime) {
      this.processingStats.minProcessingTime = processingTime;
    }
  }

  updateOffsetTracking(topic, partition, offset) {
    const key = `${topic}-${partition}`;
    this.topicPartitionOffsets.set(key, {
      topic,
      partition,
      offset: parseInt(offset),
      timestamp: Date.now()
    });
  }

  async startConsuming() {
    try {
      if (this.isRunning) {
        logger.warn('Consumer is already running');
        return;
      }

      logger.kafkaLog('Starting message consumption');
      this.isRunning = true;
      this.startTime = Date.now();

      await this.circuitBreaker.execute(async () => {
        await this.consumer.run({
          partitionsConsumedConcurrently: config.kafka.consumer.partitionConcurrency,
          eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
            if (!this.isRunning) {
              logger.debug('Skipping message processing - consumer is stopping');
              return;
            }

            if (this.isPaused) {
              logger.debug('Consumer is paused, skipping message');
              return;
            }

            try {
              await this.processMessage(message, topic, partition);
              
              // Heartbeat to keep the consumer alive
              await heartbeat();
              
            } catch (error) {
              logger.error('Error in message processing', {
                topic,
                partition,
                offset: message.offset,
                error: error.message
              });

              // Pause consumer if too many errors
              if (this.shouldPauseOnError()) {
                logger.warn('Pausing consumer due to high error rate');
                this.isPaused = true;
                pause();
                
                // Auto-resume after a delay
                setTimeout(() => {
                  this.isPaused = false;
                  logger.info('Auto-resuming consumer');
                }, 30000); // 30 seconds
              }
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
              if (!this.isRunning || !isRunning() || isStale() || this.isPaused) {
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
                resolveOffset(message.offset);
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

  shouldPauseOnError() {
    const errorRate = this.messageCount > 0 ? (this.errorCount / this.messageCount) : 0;
    const recentErrors = this.errorCount; // Could implement time-based window
    
    // Pause if error rate is above 50% or if we have more than 10 recent errors
    return errorRate > 0.5 || recentErrors > 10;
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
      this.isPaused = true;
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
      this.isPaused = false;
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

  async getConsumerGroupMetadata() {
    try {
      const admin = kafkaConfig.getAdmin();
      await admin.connect();
      
      const groupDescription = await admin.describeGroups([config.kafka.groupId]);
      await admin.disconnect();
      
      return groupDescription.groups[0];
    } catch (error) {
      logger.warn('Failed to get consumer group metadata', {
        error: error.message
      });
      return null;
    }
  }

  async getTopicMetadata(topic) {
    try {
      return await kafkaConfig.getTopicMetadata(topic);
    } catch (error) {
      logger.error('Failed to get topic metadata', {
        topic,
        error: error.message
      });
      throw error;
    }
  }

  getMetrics() {
    const uptime = Date.now() - this.startTime;
    const messagesPerSecond = this.messageCount > 0 ? 
      (this.messageCount / (uptime / 1000)).toFixed(2) : 0;

    return {
      isRunning: this.isRunning,
      isPaused: this.isPaused,
      messageCount: this.messageCount,
      errorCount: this.errorCount,
      lastMessageTime: this.lastMessageTime,
      uptime: uptime,
      messagesPerSecond: parseFloat(messagesPerSecond),
      circuitBreakerState: this.circuitBreaker.getState(),
      successRate: this.messageCount > 0 ? 
        ((this.messageCount - this.errorCount) / this.messageCount * 100).toFixed(2) : 0,
      processingStats: {
        ...this.processingStats,
        averageProcessingTime: parseFloat(this.processingStats.averageProcessingTime.toFixed(2)),
        maxProcessingTime: this.processingStats.maxProcessingTime,
        minProcessingTime: this.processingStats.minProcessingTime === Infinity ? 0 : this.processingStats.minProcessingTime
      },
      topicPartitionOffsets: Array.from(this.topicPartitionOffsets.values()),
      registeredHandlers: Array.from(this.messageHandlers.keys())
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
          paused: this.isPaused,
          messageCount: this.messageCount,
          errorCount: this.errorCount,
          lastMessageTime: this.lastMessageTime,
          successRate: metrics.successRate,
          messagesPerSecond: metrics.messagesPerSecond,
          averageProcessingTime: metrics.processingStats.averageProcessingTime
        },
        kafka: kafkaHealth,
        consumerGroup: this.consumerGroupMetadata,
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

  // Advanced features
  async getConsumerLag() {
    try {
      const admin = kafkaConfig.getAdmin();
      await admin.connect();
      
      const groupOffsets = await admin.fetchOffsets({
        groupId: config.kafka.groupId,
        topics: [{ topic: config.kafka.topic }]
      });
      
      const topicOffsets = await admin.fetchTopicOffsets(config.kafka.topic);
      
      await admin.disconnect();
      
      const lag = groupOffsets.map(groupOffset => {
        const topicOffset = topicOffsets.find(to => to.partition === groupOffset.partition);
        return {
          partition: groupOffset.partition,
          currentOffset: parseInt(groupOffset.offset),
          highWaterMark: parseInt(topicOffset.high),
          lag: parseInt(topicOffset.high) - parseInt(groupOffset.offset)
        };
      });
      
      return lag;
    } catch (error) {
      logger.error('Failed to get consumer lag', {
        error: error.message
      });
      throw error;
    }
  }

  resetMetrics() {
    this.messageCount = 0;
    this.errorCount = 0;
    this.lastMessageTime = null;
    this.startTime = Date.now();
    this.processingStats = {
      totalProcessingTime: 0,
      averageProcessingTime: 0,
      maxProcessingTime: 0,
      minProcessingTime: Infinity
    };
    this.topicPartitionOffsets.clear();
    
    logger.kafkaLog('Consumer metrics reset');
  }
}

module.exports = KafkaConsumerService;