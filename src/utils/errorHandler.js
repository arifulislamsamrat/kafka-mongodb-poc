const logger = require('./logger');
const config = require('../config');

class ErrorHandler {
  constructor() {
    this.retryDelays = [1000, 2000, 5000, 10000]; // Exponential backoff
  }

  handleKafkaError(error, context = {}) {
    const errorInfo = {
      type: 'kafka_error',
      message: error.message,
      code: error.code,
      retriable: this.isRetriableKafkaError(error),
      context
    };

    logger.error('Kafka error occurred', errorInfo);

    if (errorInfo.retriable) {
      return this.createRetryStrategy(error, context);
    }

    return { shouldRetry: false, delay: 0 };
  }

  handleMongoError(error, context = {}) {
    const errorInfo = {
      type: 'mongodb_error',
      message: error.message,
      code: error.code,
      retriable: this.isRetriableMongoError(error),
      context
    };

    logger.error('MongoDB error occurred', errorInfo);

    if (errorInfo.retriable) {
      return this.createRetryStrategy(error, context);
    }

    return { shouldRetry: false, delay: 0 };
  }

  handleProcessingError(error, message, context = {}) {
    const errorInfo = {
      type: 'processing_error',
      message: error.message,
      messageKey: message?.key?.toString(),
      messageOffset: message?.offset,
      messagePartition: message?.partition,
      retriable: this.isRetriableProcessingError(error),
      context
    };

    logger.error('Message processing error', errorInfo);

    // Log the problematic message for debugging
    if (message) {
      logger.debug('Problematic message', {
        key: message.key?.toString(),
        value: message.value?.toString(),
        headers: message.headers,
        offset: message.offset,
        partition: message.partition
      });
    }

    if (errorInfo.retriable) {
      return this.createRetryStrategy(error, context);
    }

    // Send to DLQ if enabled
    if (config.errorHandling.enableDLQ && message) {
      this.sendToDeadLetterQueue(message, error);
    }

    return { shouldRetry: false, delay: 0 };
  }

  isRetriableKafkaError(error) {
    const retriableCodes = [
      'ECONNREFUSED',
      'ENOTFOUND',
      'ETIMEDOUT',
      'NETWORK_EXCEPTION',
      'REQUEST_TIMED_OUT',
      'BROKER_NOT_AVAILABLE',
      'LEADER_NOT_AVAILABLE',
      'NOT_ENOUGH_REPLICAS',
      'NOT_ENOUGH_REPLICAS_AFTER_APPEND'
    ];

    return retriableCodes.includes(error.code) || 
           retriableCodes.some(code => error.message.includes(code));
  }

  isRetriableMongoError(error) {
    const retriableCodes = [
      'ECONNREFUSED',
      'ENOTFOUND',
      'ETIMEDOUT',
      'EPIPE',
      'ECONNRESET',
      11000, // Duplicate key error (might be retriable in some cases)
      11600, // InterruptedAtShutdown
      11602, // InterruptedDueToReplStateChange
      13435, // NotPrimaryNoSecondaryOk
      13436, // NotPrimaryOrSecondary
      189,   // PrimarySteppedDown
      91,    // ShutdownInProgress
      7,     // HostNotFound
      6,     // HostUnreachable
      89,    // NetworkTimeout
      9001   // SocketException
    ];

    return retriableCodes.includes(error.code) ||
           error.message.includes('connection') ||
           error.message.includes('timeout') ||
           error.message.includes('network');
  }

  isRetriableProcessingError(error) {
    // Add logic for determining if processing errors are retriable
    // For example, validation errors are usually not retriable
    // but temporary service unavailability might be
    
    const nonRetriablePatterns = [
      'validation',
      'invalid',
      'malformed',
      'parse error',
      'schema'
    ];

    const errorMessage = error.message.toLowerCase();
    return !nonRetriablePatterns.some(pattern => 
      errorMessage.includes(pattern)
    );
  }

  createRetryStrategy(error, context) {
    const attempt = context.attempt || 0;
    const maxRetries = config.app.maxRetries;

    if (attempt >= maxRetries) {
      logger.error('Max retries exceeded', {
        error: error.message,
        attempts: attempt + 1,
        maxRetries
      });
      return { shouldRetry: false, delay: 0 };
    }

    const delay = this.calculateRetryDelay(attempt);
    
    logger.warn('Scheduling retry', {
      error: error.message,
      attempt: attempt + 1,
      maxRetries,
      delay
    });

    return { shouldRetry: true, delay, attempt: attempt + 1 };
  }

  calculateRetryDelay(attempt) {
    // Exponential backoff with jitter
    const baseDelay = config.app.retryDelay;
    const exponentialDelay = baseDelay * Math.pow(2, attempt);
    const jitter = Math.random() * 1000; // Add up to 1 second of jitter
    
    return Math.min(exponentialDelay + jitter, 30000); // Cap at 30 seconds
  }

  async sendToDeadLetterQueue(message, error) {
    try {
      // This would typically use a separate producer to send to DLQ
      logger.info('Sending message to DLQ', {
        topic: config.errorHandling.dlqTopic,
        messageKey: message.key?.toString(),
        messageOffset: message.offset,
        error: error.message
      });

      // Implementation would go here to actually send to DLQ
      // For now, just log the action
      
    } catch (dlqError) {
      logger.error('Failed to send message to DLQ', {
        originalError: error.message,
        dlqError: dlqError.message,
        messageKey: message.key?.toString()
      });
    }
  }

  async sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  wrapWithRetry(asyncFunction, context = {}) {
    return async (...args) => {
      let lastError;
      let attempt = 0;

      while (attempt <= config.app.maxRetries) {
        try {
          return await asyncFunction.apply(this, args);
        } catch (error) {
          lastError = error;
          
          const retryInfo = this.createRetryStrategy(error, { 
            ...context, 
            attempt 
          });

          if (!retryInfo.shouldRetry) {
            throw error;
          }

          await this.sleep(retryInfo.delay);
          attempt = retryInfo.attempt;
        }
      }

      throw lastError;
    };
  }

  // Circuit breaker pattern implementation
  createCircuitBreaker(name, failureThreshold = 5, resetTimeout = 60000) {
    let failures = 0;
    let lastFailureTime = null;
    let state = 'CLOSED'; // CLOSED, OPEN, HALF_OPEN

    return {
      async execute(fn) {
        if (state === 'OPEN') {
          if (Date.now() - lastFailureTime >= resetTimeout) {
            state = 'HALF_OPEN';
            logger.info('Circuit breaker transitioning to HALF_OPEN', { name });
          } else {
            throw new Error(`Circuit breaker is OPEN for ${name}`);
          }
        }

        try {
          const result = await fn();
          
          if (state === 'HALF_OPEN') {
            state = 'CLOSED';
            failures = 0;
            logger.info('Circuit breaker CLOSED', { name });
          }
          
          return result;
        } catch (error) {
          failures++;
          lastFailureTime = Date.now();

          if (failures >= failureThreshold) {
            state = 'OPEN';
            logger.warn('Circuit breaker OPEN', { 
              name, 
              failures, 
              threshold: failureThreshold 
            });
          }

          throw error;
        }
      },

      getState() {
        return { state, failures, lastFailureTime };
      }
    };
  }
}

module.exports = new ErrorHandler();