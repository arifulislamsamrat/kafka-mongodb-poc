const Joi = require('joi');
const logger = require('./logger');

class Validator {
  constructor() {
    this.schemas = this.defineSchemas();
  }

  defineSchemas() {
    return {
      // Kafka message schema
      kafkaMessage: Joi.object({
        topic: Joi.string().required(),
        partition: Joi.number().integer().min(0).required(),
        offset: Joi.string().required(),
        timestamp: Joi.alternatives().try(
          Joi.string(),
          Joi.number().integer().positive()
        ).required(),
        key: Joi.alternatives().try(
          Joi.string(),
          Joi.binary(),
          Joi.allow(null)
        ),
        value: Joi.alternatives().try(
          Joi.string(),
          Joi.object(),
          Joi.binary()
        ).required(),
        headers: Joi.object().pattern(
          Joi.string(),
          Joi.alternatives().try(
            Joi.string(),
            Joi.binary(),
            Joi.allow(null)
          )
        ).allow(null)
      }),

      // User event schema (example business logic validation)
      userEvent: Joi.object({
        userId: Joi.alternatives().try(
          Joi.string(),
          Joi.number().integer().positive()
        ).required(),
        action: Joi.string().valid(
          'login',
          'logout',
          'purchase',
          'view_product',
          'add_to_cart',
          'remove_from_cart',
          'search',
          'signup'
        ).required(),
        timestamp: Joi.alternatives().try(
          Joi.date().iso(),
          Joi.string().isoDate(),
          Joi.number().integer().positive()
        ).required(),
        metadata: Joi.object({
          browser: Joi.string().allow(null),
          ip: Joi.string().ip().allow(null),
          sessionId: Joi.string().allow(null),
          userAgent: Joi.string().allow(null),
          referrer: Joi.string().uri().allow(null, ''),
          productId: Joi.alternatives().try(
            Joi.string(),
            Joi.number().integer().positive()
          ).allow(null),
          amount: Joi.number().positive().allow(null),
          currency: Joi.string().length(3).uppercase().allow(null),
          searchQuery: Joi.string().allow(null)
        }).allow(null)
      }),

      // MongoDB document schema
      mongoDocument: Joi.object({
        partition: Joi.number().integer().min(0).required(),
        offset: Joi.string().required(),
        timestamp: Joi.date().required(),
        key: Joi.string().allow(null),
        value: Joi.object().required(),
        headers: Joi.object().allow(null),
        receivedAt: Joi.date().required(),
        processedAt: Joi.date().allow(null),
        retryCount: Joi.number().integer().min(0).default(0)
      }),

      // Batch processing schema
      batch: Joi.object({
        messages: Joi.array().items(
          Joi.object().required()
        ).min(1).max(1000).required(),
        batchId: Joi.string().uuid().required(),
        createdAt: Joi.date().required(),
        size: Joi.number().integer().positive().required()
      }),

      // Configuration validation
      config: Joi.object({
        batchSize: Joi.number().integer().min(1).max(10000),
        batchTimeout: Joi.number().integer().min(100).max(300000),
        maxRetries: Joi.number().integer().min(0).max(10),
        retryDelay: Joi.number().integer().min(100).max(60000)
      })
    };
  }

  validateKafkaMessage(message) {
    const { error, value } = this.schemas.kafkaMessage.validate(message, {
      allowUnknown: true,
      stripUnknown: false
    });

    if (error) {
      logger.error('Kafka message validation failed', {
        error: error.details,
        message: this.sanitizeMessage(message)
      });
      throw new Error(`Invalid Kafka message: ${error.message}`);
    }

    return value;
  }

  validateUserEvent(eventData) {
    const { error, value } = this.schemas.userEvent.validate(eventData, {
      allowUnknown: true,
      stripUnknown: false
    });

    if (error) {
      logger.error('User event validation failed', {
        error: error.details,
        event: this.sanitizeEvent(eventData)
      });
      throw new Error(`Invalid user event: ${error.message}`);
    }

    return value;
  }

  validateMongoDocument(document) {
    const { error, value } = this.schemas.mongoDocument.validate(document, {
      allowUnknown: true,
      stripUnknown: false
    });

    if (error) {
      logger.error('MongoDB document validation failed', {
        error: error.details,
        document: this.sanitizeDocument(document)
      });
      throw new Error(`Invalid MongoDB document: ${error.message}`);
    }

    return value;
  }

  validateBatch(batch) {
    const { error, value } = this.schemas.batch.validate(batch);

    if (error) {
      logger.error('Batch validation failed', {
        error: error.details,
        batchSize: batch?.messages?.length
      });
      throw new Error(`Invalid batch: ${error.message}`);
    }

    return value;
  }

  validateConfig(config) {
    const { error, value } = this.schemas.config.validate(config);

    if (error) {
      logger.error('Configuration validation failed', {
        error: error.details
      });
      throw new Error(`Invalid configuration: ${error.message}`);
    }

    return value;
  }

  // Message transformation and enrichment
  enrichMessage(kafkaMessage) {
    try {
      const enriched = {
        ...kafkaMessage,
        receivedAt: new Date(),
        processedAt: null,
        retryCount: 0,
        id: this.generateMessageId(kafkaMessage)
      };

      // Parse timestamp if it's a string or number
      if (typeof enriched.timestamp === 'string') {
        enriched.timestamp = new Date(enriched.timestamp);
      } else if (typeof enriched.timestamp === 'number') {
        enriched.timestamp = new Date(enriched.timestamp);
      }

      // Parse JSON value if it's a string
      if (typeof enriched.value === 'string') {
        try {
          enriched.value = JSON.parse(enriched.value);
        } catch (parseError) {
          logger.warn('Failed to parse message value as JSON', {
            messageId: enriched.id,
            error: parseError.message
          });
          // Keep as string if parsing fails
        }
      }

      // Validate the enriched message
      this.validateMongoDocument(enriched);

      return enriched;
    } catch (error) {
      logger.error('Failed to enrich message', {
        error: error.message,
        message: this.sanitizeMessage(kafkaMessage)
      });
      throw error;
    }
  }

  generateMessageId(message) {
    return `${message.topic}-${message.partition}-${message.offset}`;
  }

  // Sanitization methods to avoid logging sensitive data
  sanitizeMessage(message) {
    if (!message) return null;
    
    return {
      topic: message.topic,
      partition: message.partition,
      offset: message.offset,
      timestamp: message.timestamp,
      key: message.key ? '[REDACTED]' : null,
      valueType: typeof message.value,
      hasHeaders: !!message.headers
    };
  }

  sanitizeEvent(event) {
    if (!event) return null;
    
    return {
      userId: event.userId ? '[REDACTED]' : null,
      action: event.action,
      timestamp: event.timestamp,
      hasMetadata: !!event.metadata
    };
  }

  sanitizeDocument(document) {
    if (!document) return null;
    
    return {
      partition: document.partition,
      offset: document.offset,
      timestamp: document.timestamp,
      hasValue: !!document.value,
      receivedAt: document.receivedAt
    };
  }

  // Schema utilities
  addCustomSchema(name, schema) {
    this.schemas[name] = schema;
    logger.info('Custom schema added', { name });
  }

  getSchema(name) {
    return this.schemas[name];
  }

  listSchemas() {
    return Object.keys(this.schemas);
  }

  // Validation utilities
  isValidJSON(str) {
    try {
      JSON.parse(str);
      return true;
    } catch {
      return false;
    }
  }

  isValidISO8601(dateStr) {
    try {
      const date = new Date(dateStr);
      return date.toISOString() === dateStr;
    } catch {
      return false;
    }
  }

  isValidUUID(str) {
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    return uuidRegex.test(str);
  }

  // Message filtering
  shouldProcessMessage(message) {
    try {
      // Basic checks
      if (!message || !message.value) {
        logger.debug('Skipping message: no value', {
          messageId: this.generateMessageId(message)
        });
        return false;
      }

      // Check for test messages (optional)
      if (message.headers && message.headers['test-message']) {
        logger.debug('Skipping test message', {
          messageId: this.generateMessageId(message)
        });
        return false;
      }

      // Additional business logic filters can be added here
      
      return true;
    } catch (error) {
      logger.error('Error in message filtering', {
        error: error.message,
        message: this.sanitizeMessage(message)
      });
      return false;
    }
  }
}

module.exports = new Validator();