const logger = require('../utils/logger');
const validator = require('../utils/validator');
const errorHandler = require('../utils/errorHandler');
const config = require('../config');
const { v4: uuidv4 } = require('crypto').randomUUID || require('uuid').v4;

class BatchProcessor {
  constructor(mongodbService) {
    this.mongodbService = mongodbService;
    this.batchSize = config.app.batchSize;
    this.batchTimeout = config.app.batchTimeout;
    this.pendingBatches = new Map();
    this.processedBatches = 0;
    this.processedMessages = 0;
    this.errorCount = 0;
    this.batchTimers = new Map();
    this.isEnabled = true;
  }

  async processBatch(messages) {
    const batchId = uuidv4();
    const startTime = Date.now();

    try {
      logger.info('Processing batch', {
        batchId,
        messageCount: messages.length,
        batchSize: this.batchSize
      });

      // Validate batch
      this.validateBatch(messages);

      // Pre-process all messages
      const processedMessages = await this.preprocessMessages(messages, batchId);

      // Filter valid messages
      const validMessages = processedMessages.filter(msg => msg !== null);

      if (validMessages.length === 0) {
        logger.warn('No valid messages in batch after preprocessing', { batchId });
        return { batchId, processed: 0, skipped: messages.length, errors: 0 };
      }

      // Store batch in MongoDB
      const result = await this.mongodbService.insertBatch(validMessages);

      // Update metrics
      this.processedBatches++;
      this.processedMessages += result.insertedCount || validMessages.length;

      const processingTime = Date.now() - startTime;

      logger.info('Batch processed successfully', {
        batchId,
        messageCount: messages.length,
        processed: result.insertedCount || validMessages.length,
        skipped: messages.length - validMessages.length,
        duplicates: result.duplicateCount || 0,
        processingTime: `${processingTime}ms`,
        avgTimePerMessage: `${(processingTime / validMessages.length).toFixed(2)}ms`
      });

      return {
        batchId,
        processed: result.insertedCount || validMessages.length,
        skipped: messages.length - validMessages.length,
        duplicates: result.duplicateCount || 0,
        errors: 0,
        processingTime
      };

    } catch (error) {
      this.errorCount++;
      const processingTime = Date.now() - startTime;

      logger.error('Batch processing failed', {
        batchId,
        messageCount: messages.length,
        error: error.message,
        processingTime: `${processingTime}ms`
      });

      // Handle batch processing error
      await this.handleBatchError(batchId, messages, error);

      return {
        batchId,
        processed: 0,
        skipped: 0,
        duplicates: 0,
        errors: messages.length,
        processingTime,
        error: error.message
      };
    }
  }

  async preprocessMessages(messages, batchId) {
    const processedMessages = [];

    for (let i = 0; i < messages.length; i++) {
      try {
        const message = messages[i];
        
        // Check if message should be processed
        if (!validator.shouldProcessMessage(message)) {
          logger.debug('Message filtered out during batch preprocessing', {
            batchId,
            messageIndex: i,
            messageId: validator.generateMessageId(message)
          });
          processedMessages.push(null);
          continue;
        }

        // Enrich and validate message
        const enrichedMessage = validator.enrichMessage(message);
        
        // Add batch metadata
        enrichedMessage.batchId = batchId;
        enrichedMessage.batchIndex = i;
        enrichedMessage.batchSize = messages.length;

        processedMessages.push(enrichedMessage);

      } catch (error) {
        logger.warn('Failed to preprocess message in batch', {
          batchId,
          messageIndex: i,
          error: error.message
        });
        
        // Add null to maintain array indexing
        processedMessages.push(null);
      }
    }

    return processedMessages;
  }

  validateBatch(messages) {
    if (!Array.isArray(messages)) {
      throw new Error('Messages must be an array');
    }

    if (messages.length === 0) {
      throw new Error('Batch cannot be empty');
    }

    if (messages.length > this.batchSize * 2) {
      logger.warn('Batch size exceeds recommended maximum', {
        messageCount: messages.length,
        recommendedMax: this.batchSize * 2
      });
    }

    // Validate batch structure
    validator.validateBatch({
      messages: messages,
      batchId: uuidv4(),
      createdAt: new Date(),
      size: messages.length
    });
  }

  async handleBatchError(batchId, messages, error) {
    try {
      // Determine if error is retriable
      const retryInfo = errorHandler.handleProcessingError(error, null, {
        batchId,
        messageCount: messages.length
      });

      if (retryInfo.shouldRetry) {
        logger.info('Scheduling batch retry', {
          batchId,
          attempt: retryInfo.attempt,
          delay: retryInfo.delay
        });

        // Schedule retry after delay
        setTimeout(async () => {
          try {
            await this.processBatch(messages);
          } catch (retryError) {
            logger.error('Batch retry failed', {
              batchId,
              error: retryError.message
            });
          }
        }, retryInfo.delay);

      } else {
        // Process messages individually as fallback
        await this.fallbackToIndividualProcessing(batchId, messages);
      }

    } catch (handlingError) {
      logger.error('Failed to handle batch error', {
        batchId,
        originalError: error.message,
        handlingError: handlingError.message
      });
    }
  }

  async fallbackToIndividualProcessing(batchId, messages) {
    logger.info('Falling back to individual message processing', {
      batchId,
      messageCount: messages.length
    });

    const results = {
      processed: 0,
      failed: 0,
      skipped: 0
    };

    for (let i = 0; i < messages.length; i++) {
      try {
        const message = messages[i];
        
        if (!validator.shouldProcessMessage(message)) {
          results.skipped++;
          continue;
        }

        const enrichedMessage = validator.enrichMessage(message);
        await this.mongodbService.insertMessage(enrichedMessage);
        results.processed++;

      } catch (error) {
        results.failed++;
        logger.warn('Individual message processing failed in fallback', {
          batchId,
          messageIndex: i,
          error: error.message
        });
      }
    }

    logger.info('Fallback processing completed', {
      batchId,
      ...results
    });

    return results;
  }

  // Streaming batch processor for continuous processing
  createStreamingProcessor() {
    const pendingMessages = [];
    let timeoutId = null;

    const processPendingBatch = async () => {
      if (pendingMessages.length === 0) return;

      const messagesToProcess = pendingMessages.splice(0);
      if (timeoutId) {
        clearTimeout(timeoutId);
        timeoutId = null;
      }

      try {
        await this.processBatch(messagesToProcess);
      } catch (error) {
        logger.error('Streaming batch processing failed', {
          messageCount: messagesToProcess.length,
          error: error.message
        });
      }
    };

    const scheduleTimeout = () => {
      if (timeoutId) clearTimeout(timeoutId);
      timeoutId = setTimeout(processPendingBatch, this.batchTimeout);
    };

    return {
      addMessage: async (message) => {
        if (!this.isEnabled) {
          // Process immediately if batching is disabled
          return await this.processBatch([message]);
        }

        pendingMessages.push(message);

        // Process when batch is full
        if (pendingMessages.length >= this.batchSize) {
          await processPendingBatch();
        } else {
          // Schedule timeout processing
          scheduleTimeout();
        }
      },

      flush: async () => {
        await processPendingBatch();
      },

      getPendingCount: () => pendingMessages.length,

      stop: () => {
        if (timeoutId) {
          clearTimeout(timeoutId);
          timeoutId = null;
        }
      }
    };
  }

  // Advanced batch processing with priority queues
  createPriorityBatchProcessor() {
    const priorityQueues = {
      high: [],
      normal: [],
      low: []
    };

    const processPriorityBatch = async (priority) => {
      const queue = priorityQueues[priority];
      if (queue.length === 0) return;

      const batchSize = this.getBatchSizeForPriority(priority);
      const messagesToProcess = queue.splice(0, batchSize);

      logger.debug('Processing priority batch', {
        priority,
        messageCount: messagesToProcess.length
      });

      return await this.processBatch(messagesToProcess);
    };

    return {
      addMessage: async (message, priority = 'normal') => {
        if (!priorityQueues[priority]) {
          priority = 'normal';
        }

        priorityQueues[priority].push(message);

        // Check if any queue is ready for processing
        for (const [queuePriority, queue] of Object.entries(priorityQueues)) {
          const batchSize = this.getBatchSizeForPriority(queuePriority);
          if (queue.length >= batchSize) {
            await processPriorityBatch(queuePriority);
            break;
          }
        }
      },

      processAllQueues: async () => {
        const results = {};
        for (const priority of ['high', 'normal', 'low']) {
          if (priorityQueues[priority].length > 0) {
            results[priority] = await processPriorityBatch(priority);
          }
        }
        return results;
      },

      getQueueSizes: () => {
        return Object.fromEntries(
          Object.entries(priorityQueues).map(([priority, queue]) => [priority, queue.length])
        );
      }
    };
  }

  getBatchSizeForPriority(priority) {
    switch (priority) {
      case 'high': return Math.floor(this.batchSize * 0.5); // Smaller batches for faster processing
      case 'normal': return this.batchSize;
      case 'low': return Math.floor(this.batchSize * 1.5); // Larger batches for efficiency
      default: return this.batchSize;
    }
  }

  // Configuration management
  updateBatchSize(newSize) {
    if (newSize < 1 || newSize > 10000) {
      throw new Error('Batch size must be between 1 and 10000');
    }
    
    const oldSize = this.batchSize;
    this.batchSize = newSize;
    
    logger.info('Batch size updated', {
      from: oldSize,
      to: newSize
    });
  }

  updateBatchTimeout(newTimeout) {
    if (newTimeout < 100 || newTimeout > 300000) {
      throw new Error('Batch timeout must be between 100ms and 300s');
    }
    
    const oldTimeout = this.batchTimeout;
    this.batchTimeout = newTimeout;
    
    logger.info('Batch timeout updated', {
      from: oldTimeout,
      to: newTimeout
    });
  }

  enableBatching() {
    this.isEnabled = true;
    logger.info('Batch processing enabled');
  }

  disableBatching() {
    this.isEnabled = false;
    logger.info('Batch processing disabled - will process messages individually');
  }

  // Metrics and monitoring
  getMetrics() {
    return {
      isEnabled: this.isEnabled,
      batchSize: this.batchSize,
      batchTimeout: this.batchTimeout,
      processedBatches: this.processedBatches,
      processedMessages: this.processedMessages,
      errorCount: this.errorCount,
      pendingBatches: this.pendingBatches.size,
      averageMessagesPerBatch: this.processedBatches > 0 ? 
        (this.processedMessages / this.processedBatches).toFixed(2) : 0,
      successRate: this.processedBatches > 0 ? 
        (((this.processedBatches - this.errorCount) / this.processedBatches) * 100).toFixed(2) : 0
    };
  }

  resetMetrics() {
    this.processedBatches = 0;
    this.processedMessages = 0;
    this.errorCount = 0;
    logger.info('Batch processor metrics reset');
  }

  // Health check
  async healthCheck() {
    try {
      const metrics = this.getMetrics();
      const mongoHealth = await this.mongodbService.healthCheck();
      
      return {
        status: mongoHealth.status === 'healthy' && this.isEnabled ? 'healthy' : 'degraded',
        batchProcessor: {
          enabled: this.isEnabled,
          batchSize: this.batchSize,
          processedBatches: this.processedBatches,
          processedMessages: this.processedMessages,
          successRate: metrics.successRate
        },
        mongodb: mongoHealth,
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

module.exports = BatchProcessor;