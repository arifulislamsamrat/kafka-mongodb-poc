const Joi = require('joi');

/**
 * Message Model
 * Defines data structures, validation schemas, and transformation logic
 * for Kafka messages and their MongoDB representations
 */

class MessageModel {
  constructor() {
    this.schemas = this.defineSchemas();
  }

  defineSchemas() {
    return {
      // Raw Kafka message schema
      kafkaMessage: Joi.object({
        topic: Joi.string().required(),
        partition: Joi.number().integer().min(0).required(),
        offset: Joi.string().required(),
        timestamp: Joi.alternatives().try(
          Joi.date(),
          Joi.string().isoDate(),
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

      // User event payload schema
      userEvent: Joi.object({
        userId: Joi.alternatives().try(
          Joi.string().min(1),
          Joi.number().integer().positive()
        ).required(),
        action: Joi.string().valid(
          'login',
          'logout',
          'signup',
          'purchase',
          'view_product',
          'add_to_cart',
          'remove_from_cart',
          'search',
          'wishlist_add',
          'wishlist_remove',
          'review_submit',
          'profile_update',
          'password_change',
          'subscription_start',
          'subscription_cancel'
        ).required(),
        timestamp: Joi.alternatives().try(
          Joi.date(),
          Joi.string().isoDate(),
          Joi.number().integer().positive()
        ).required(),
        metadata: Joi.object({
          // User context
          sessionId: Joi.string().allow(null),
          browser: Joi.string().allow(null),
          device: Joi.string().valid('desktop', 'mobile', 'tablet').allow(null),
          userAgent: Joi.string().allow(null),
          ip: Joi.string().ip().allow(null),
          country: Joi.string().length(2).uppercase().allow(null),
          language: Joi.string().allow(null),
          timezone: Joi.string().allow(null),
          
          // Referral context
          referrer: Joi.string().uri().allow(null, ''),
          campaign: Joi.string().allow(null),
          source: Joi.string().allow(null),
          medium: Joi.string().allow(null),
          
          // Product context (for commerce events)
          productId: Joi.alternatives().try(
            Joi.string(),
            Joi.number().integer().positive()
          ).allow(null),
          category: Joi.string().allow(null),
          subcategory: Joi.string().allow(null),
          brand: Joi.string().allow(null),
          price: Joi.number().positive().allow(null),
          currency: Joi.string().length(3).uppercase().allow(null),
          quantity: Joi.number().integer().positive().allow(null),
          
          // Transaction context
          orderId: Joi.string().allow(null),
          amount: Joi.number().positive().allow(null),
          paymentMethod: Joi.string().allow(null),
          
          // Search context
          searchQuery: Joi.string().allow(null),
          resultsCount: Joi.number().integer().min(0).allow(null),
          searchFilters: Joi.object().allow(null),
          
          // Additional metadata
          experimentId: Joi.string().allow(null),
          variant: Joi.string().allow(null),
          previousPage: Joi.string().allow(null),
          nextPage: Joi.string().allow(null)
        }).allow(null)
      }),

      // System event payload schema
      systemEvent: Joi.object({
        eventType: Joi.string().valid(
          'application_start',
          'application_stop',
          'health_check',
          'error_occurred',
          'performance_metric',
          'configuration_change',
          'database_event',
          'kafka_event'
        ).required(),
        timestamp: Joi.alternatives().try(
          Joi.date(),
          Joi.string().isoDate(),
          Joi.number().integer().positive()
        ).required(),
        source: Joi.string().required(),
        level: Joi.string().valid('debug', 'info', 'warn', 'error', 'critical').required(),
        message: Joi.string().required(),
        metadata: Joi.object({
          component: Joi.string().allow(null),
          version: Joi.string().allow(null),
          environment: Joi.string().allow(null),
          hostname: Joi.string().allow(null),
          processId: Joi.number().integer().allow(null),
          correlationId: Joi.string().allow(null),
          traceId: Joi.string().allow(null),
          duration: Joi.number().positive().allow(null),
          errorCode: Joi.string().allow(null),
          stackTrace: Joi.string().allow(null)
        }).allow(null)
      }),

      // Enriched MongoDB document schema
      mongoDocument: Joi.object({
        // Original Kafka metadata
        topic: Joi.string().required(),
        partition: Joi.number().integer().min(0).required(),
        offset: Joi.string().required(),
        timestamp: Joi.date().required(),
        key: Joi.string().allow(null),
        value: Joi.object().required(),
        headers: Joi.object().allow(null),
        
        // Enrichment metadata
        id: Joi.string().required(), // Generated unique ID
        receivedAt: Joi.date().required(),
        processedAt: Joi.date().allow(null),
        
        // Processing metadata
        retryCount: Joi.number().integer().min(0).default(0),
        batchId: Joi.string().allow(null),
        batchIndex: Joi.number().integer().min(0).allow(null),
        batchSize: Joi.number().integer().positive().allow(null),
        
        // Data classification
        messageType: Joi.string().allow(null),
        priority: Joi.string().valid('low', 'normal', 'high', 'critical').default('normal'),
        
        // Quality metrics
        validationStatus: Joi.string().valid('valid', 'warning', 'error').default('valid'),
        validationErrors: Joi.array().items(Joi.string()).allow(null),
        
        // Audit trail
        createdBy: Joi.string().default('kafka-consumer'),
        updatedAt: Joi.date().allow(null),
        updatedBy: Joi.string().allow(null)
      })
    };
  }

  // Factory methods for creating different message types
  createUserEvent(userId, action, metadata = {}) {
    const event = {
      userId,
      action,
      timestamp: new Date().toISOString(),
      metadata: {
        sessionId: null,
        browser: null,
        device: null,
        userAgent: null,
        ip: null,
        country: null,
        language: null,
        timezone: null,
        referrer: null,
        campaign: null,
        source: null,
        medium: null,
        ...metadata
      }
    };

    // Validate the created event
    const { error, value } = this.schemas.userEvent.validate(event);
    if (error) {
      throw new Error(`Invalid user event: ${error.message}`);
    }

    return value;
  }

  createSystemEvent(eventType, source, level, message, metadata = {}) {
    const event = {
      eventType,
      source,
      level,
      message,
      timestamp: new Date().toISOString(),
      metadata: {
        component: null,
        version: process.env.npm_package_version || '1.0.0',
        environment: process.env.NODE_ENV || 'development',
        hostname: require('os').hostname(),
        processId: process.pid,
        ...metadata
      }
    };

    // Validate the created event
    const { error, value } = this.schemas.systemEvent.validate(event);
    if (error) {
      throw new Error(`Invalid system event: ${error.message}`);
    }

    return value;
  }

  createKafkaMessage(topic, partition, offset, timestamp, key, value, headers = null) {
    const message = {
      topic,
      partition,
      offset,
      timestamp,
      key,
      value,
      headers
    };

    // Validate the created message
    const { error, value: validatedMessage } = this.schemas.kafkaMessage.validate(message);
    if (error) {
      throw new Error(`Invalid Kafka message: ${error.message}`);
    }

    return validatedMessage;
  }

  // Transformation methods
  enrichKafkaMessage(kafkaMessage, additionalMetadata = {}) {
    // Generate unique ID
    const id = this.generateMessageId(kafkaMessage);
    
    // Parse timestamp if needed
    let timestamp = kafkaMessage.timestamp;
    if (typeof timestamp === 'string') {
      timestamp = new Date(timestamp);
    } else if (typeof timestamp === 'number') {
      timestamp = new Date(timestamp);
    }

    // Parse value if it's a JSON string
    let value = kafkaMessage.value;
    if (typeof value === 'string') {
      try {
        value = JSON.parse(value);
      } catch (e) {
        // Keep as string if not valid JSON
      }
    }

    // Determine message type
    const messageType = this.determineMessageType(kafkaMessage, value);

    // Create enriched document
    const enrichedMessage = {
      // Original Kafka metadata
      topic: kafkaMessage.topic,
      partition: kafkaMessage.partition,
      offset: kafkaMessage.offset,
      timestamp: timestamp,
      key: kafkaMessage.key,
      value: value,
      headers: kafkaMessage.headers,
      
      // Enrichment metadata
      id: id,
      receivedAt: new Date(),
      processedAt: null,
      
      // Processing metadata
      retryCount: 0,
      batchId: additionalMetadata.batchId || null,
      batchIndex: additionalMetadata.batchIndex || null,
      batchSize: additionalMetadata.batchSize || null,
      
      // Data classification
      messageType: messageType,
      priority: this.determinePriority(value, messageType),
      
      // Quality metrics
      validationStatus: 'valid',
      validationErrors: null,
      
      // Audit trail
      createdBy: 'kafka-consumer',
      updatedAt: null,
      updatedBy: null
    };

    // Validate enriched message
    const { error, value: validatedDocument } = this.schemas.mongoDocument.validate(enrichedMessage);
    if (error) {
      throw new Error(`Invalid enriched message: ${error.message}`);
    }

    return validatedDocument;
  }

  generateMessageId(kafkaMessage) {
    return `${kafkaMessage.topic}-${kafkaMessage.partition}-${kafkaMessage.offset}`;
  }

  determineMessageType(kafkaMessage, value) {
    // Check headers first
    if (kafkaMessage.headers && kafkaMessage.headers.messageType) {
      return kafkaMessage.headers.messageType.toString();
    }

    // Check value structure
    if (value && typeof value === 'object') {
      if (value.action && value.userId) {
        return 'user_event';
      }
      if (value.eventType && value.source) {
        return 'system_event';
      }
      if (value.type) {
        return value.type;
      }
    }

    // Default to topic name or unknown
    return kafkaMessage.topic || 'unknown';
  }

  determinePriority(value, messageType) {
    // System events priority
    if (messageType === 'system_event' && value.level) {
      switch (value.level) {
        case 'critical': return 'critical';
        case 'error': return 'high';
        case 'warn': return 'normal';
        default: return 'low';
      }
    }

    // User events priority
    if (messageType === 'user_event' && value.action) {
      switch (value.action) {
        case 'purchase':
        case 'signup':
        case 'password_change':
          return 'high';
        case 'login':
        case 'logout':
        case 'add_to_cart':
          return 'normal';
        default:
          return 'low';
      }
    }

    return 'normal';
  }

  // Validation methods
  validateUserEvent(event) {
    const { error, value } = this.schemas.userEvent.validate(event, {
      allowUnknown: true,
      stripUnknown: false
    });

    if (error) {
      throw new Error(`Invalid user event: ${error.message}`);
    }

    return value;
  }

  validateSystemEvent(event) {
    const { error, value } = this.schemas.systemEvent.validate(event, {
      allowUnknown: true,
      stripUnknown: false
    });

    if (error) {
      throw new Error(`Invalid system event: ${error.message}`);
    }

    return value;
  }

  validateKafkaMessage(message) {
    const { error, value } = this.schemas.kafkaMessage.validate(message, {
      allowUnknown: true,
      stripUnknown: false
    });

    if (error) {
      throw new Error(`Invalid Kafka message: ${error.message}`);
    }

    return value;
  }

  validateMongoDocument(document) {
    const { error, value } = this.schemas.mongoDocument.validate(document, {
      allowUnknown: true,
      stripUnknown: false
    });

    if (error) {
      throw new Error(`Invalid MongoDB document: ${error.message}`);
    }

    return value;
  }

  // Helper methods for message analysis
  extractUserContext(message) {
    if (message.value && message.value.metadata) {
      return {
        userId: message.value.userId,
        sessionId: message.value.metadata.sessionId,
        browser: message.value.metadata.browser,
        device: message.value.metadata.device,
        ip: message.value.metadata.ip,
        country: message.value.metadata.country
      };
    }
    return null;
  }

  extractBusinessContext(message) {
    if (message.value && message.value.metadata) {
      return {
        productId: message.value.metadata.productId,
        category: message.value.metadata.category,
        amount: message.value.metadata.amount,
        currency: message.value.metadata.currency,
        orderId: message.value.metadata.orderId
      };
    }
    return null;
  }

  isRetriableMessage(message) {
    // Messages that failed due to validation errors are usually not retriable
    if (message.validationStatus === 'error') {
      return false;
    }

    // High priority messages should be retried more aggressively
    if (message.priority === 'critical' || message.priority === 'high') {
      return message.retryCount < 5;
    }

    // Normal retry logic
    return message.retryCount < 3;
  }

  shouldArchiveMessage(message) {
    const ageInDays = (Date.now() - new Date(message.timestamp).getTime()) / (1000 * 60 * 60 * 24);
    
    // Archive old messages based on priority
    switch (message.priority) {
      case 'critical': return ageInDays > 365; // Keep critical messages for 1 year
      case 'high': return ageInDays > 180;     // Keep high priority for 6 months
      case 'normal': return ageInDays > 90;    // Keep normal messages for 3 months
      case 'low': return ageInDays > 30;       // Keep low priority for 1 month
      default: return ageInDays > 90;
    }
  }

  // Message transformation utilities
  transformForAnalytics(message) {
    const userContext = this.extractUserContext(message);
    const businessContext = this.extractBusinessContext(message);
    
    return {
      messageId: message.id,
      timestamp: message.timestamp,
      messageType: message.messageType,
      priority: message.priority,
      topic: message.topic,
      partition: message.partition,
      
      // User analytics
      userId: userContext?.userId,
      sessionId: userContext?.sessionId,
      device: userContext?.device,
      country: userContext?.country,
      
      // Business analytics
      action: message.value?.action,
      productId: businessContext?.productId,
      category: businessContext?.category,
      amount: businessContext?.amount,
      currency: businessContext?.currency,
      
      // Processing metadata
      processingLatency: message.processedAt ? 
        new Date(message.processedAt) - new Date(message.receivedAt) : null,
      retryCount: message.retryCount
    };
  }

  transformForAudit(message) {
    return {
      messageId: message.id,
      timestamp: message.timestamp,
      topic: message.topic,
      partition: message.partition,
      offset: message.offset,
      messageType: message.messageType,
      priority: message.priority,
      
      // Audit trail
      receivedAt: message.receivedAt,
      processedAt: message.processedAt,
      createdBy: message.createdBy,
      updatedBy: message.updatedBy,
      updatedAt: message.updatedAt,
      
      // Data integrity
      validationStatus: message.validationStatus,
      validationErrors: message.validationErrors,
      retryCount: message.retryCount,
      
      // Business context (sanitized)
      userId: message.value?.userId ? 'present' : 'absent',
      hasMetadata: !!message.value?.metadata,
      headerCount: message.headers ? Object.keys(message.headers).length : 0
    };
  }

  // Data quality methods
  assessDataQuality(message) {
    const quality = {
      score: 100,
      issues: [],
      category: 'excellent'
    };

    // Check required fields
    if (!message.value || typeof message.value !== 'object') {
      quality.score -= 30;
      quality.issues.push('Missing or invalid value object');
    }

    // Check timestamp validity
    const timestamp = new Date(message.timestamp);
    if (isNaN(timestamp.getTime())) {
      quality.score -= 20;
      quality.issues.push('Invalid timestamp');
    } else {
      // Check if timestamp is too far in the future or past
      const now = Date.now();
      const messageTime = timestamp.getTime();
      const hourInMs = 60 * 60 * 1000;
      
      if (messageTime > now + hourInMs) {
        quality.score -= 10;
        quality.issues.push('Timestamp too far in future');
      } else if (messageTime < now - (24 * hourInMs)) {
        quality.score -= 5;
        quality.issues.push('Timestamp older than 24 hours');
      }
    }

    // Check for user event specific quality
    if (message.messageType === 'user_event') {
      const userEvent = message.value;
      
      if (!userEvent.userId) {
        quality.score -= 25;
        quality.issues.push('Missing userId in user event');
      }
      
      if (!userEvent.action) {
        quality.score -= 25;
        quality.issues.push('Missing action in user event');
      }
      
      // Check metadata completeness
      if (!userEvent.metadata) {
        quality.score -= 10;
        quality.issues.push('Missing metadata object');
      } else {
        const metadata = userEvent.metadata;
        let metadataScore = 0;
        
        if (metadata.sessionId) metadataScore += 2;
        if (metadata.browser) metadataScore += 2;
        if (metadata.device) metadataScore += 2;
        if (metadata.ip) metadataScore += 2;
        if (metadata.country) metadataScore += 2;
        
        if (metadataScore < 6) {
          quality.score -= (10 - metadataScore);
          quality.issues.push('Incomplete metadata');
        }
      }
    }

    // Determine quality category
    if (quality.score >= 90) {
      quality.category = 'excellent';
    } else if (quality.score >= 80) {
      quality.category = 'good';
    } else if (quality.score >= 70) {
      quality.category = 'fair';
    } else if (quality.score >= 60) {
      quality.category = 'poor';
    } else {
      quality.category = 'critical';
    }

    return quality;
  }

  // Message sanitization for logging/debugging
  sanitizeMessage(message, level = 'basic') {
    const sanitized = {
      id: message.id,
      messageType: message.messageType,
      topic: message.topic,
      partition: message.partition,
      timestamp: message.timestamp,
      priority: message.priority,
      validationStatus: message.validationStatus
    };

    if (level === 'detailed') {
      sanitized.hasKey = !!message.key;
      sanitized.hasHeaders = !!message.headers;
      sanitized.headerCount = message.headers ? Object.keys(message.headers).length : 0;
      sanitized.retryCount = message.retryCount;
      sanitized.batchId = message.batchId;
      
      // Include sanitized value info
      if (message.value) {
        sanitized.value = {
          type: typeof message.value,
          hasUserId: !!message.value.userId,
          hasAction: !!message.value.action,
          hasMetadata: !!message.value.metadata,
          metadataKeys: message.value.metadata ? Object.keys(message.value.metadata).length : 0
        };
      }
    }

    return sanitized;
  }

  // Statistical analysis helpers
  generateMessageFingerprint(message) {
    // Create a hash-like fingerprint for deduplication and analysis
    const components = [
      message.topic,
      message.messageType,
      message.value?.action || '',
      message.value?.userId || '',
      Math.floor(new Date(message.timestamp).getTime() / 60000) // Minute precision
    ];
    
    return Buffer.from(components.join('|')).toString('base64');
  }

  classifyMessagePattern(message) {
    const patterns = [];
    
    // Time-based patterns
    const hour = new Date(message.timestamp).getHours();
    if (hour >= 9 && hour <= 17) {
      patterns.push('business_hours');
    } else if (hour >= 0 && hour <= 6) {
      patterns.push('night_activity');
    } else {
      patterns.push('evening_activity');
    }
    
    // User behavior patterns
    if (message.messageType === 'user_event') {
      const action = message.value?.action;
      switch (action) {
        case 'login':
        case 'signup':
          patterns.push('authentication');
          break;
        case 'purchase':
        case 'add_to_cart':
          patterns.push('commerce');
          break;
        case 'view_product':
        case 'search':
          patterns.push('browsing');
          break;
        default:
          patterns.push('other_interaction');
      }
    }
    
    // Device patterns
    const device = message.value?.metadata?.device;
    if (device) {
      patterns.push(`device_${device}`);
    }
    
    return patterns;
  }

  // Export/Import utilities
  exportToJSON(message, includeMetadata = true) {
    const exported = {
      id: message.id,
      topic: message.topic,
      partition: message.partition,
      offset: message.offset,
      timestamp: message.timestamp,
      key: message.key,
      value: message.value,
      headers: message.headers
    };

    if (includeMetadata) {
      exported.metadata = {
        messageType: message.messageType,
        priority: message.priority,
        receivedAt: message.receivedAt,
        processedAt: message.processedAt,
        retryCount: message.retryCount,
        validationStatus: message.validationStatus,
        validationErrors: message.validationErrors
      };
    }

    return JSON.stringify(exported, null, 2);
  }

  importFromJSON(jsonString) {
    try {
      const parsed = JSON.parse(jsonString);
      
      // Reconstruct message with validation
      const kafkaMessage = {
        topic: parsed.topic,
        partition: parsed.partition,
        offset: parsed.offset,
        timestamp: parsed.timestamp,
        key: parsed.key,
        value: parsed.value,
        headers: parsed.headers
      };

      // Validate and enrich
      this.validateKafkaMessage(kafkaMessage);
      return this.enrichKafkaMessage(kafkaMessage, parsed.metadata || {});
      
    } catch (error) {
      throw new Error(`Failed to import message from JSON: ${error.message}`);
    }
  }

  // Schema management
  getSchema(schemaName) {
    return this.schemas[schemaName];
  }

  addCustomSchema(name, schema) {
    this.schemas[name] = schema;
  }

  validateWithCustomSchema(data, schemaName) {
    if (!this.schemas[schemaName]) {
      throw new Error(`Schema '${schemaName}' not found`);
    }

    const { error, value } = this.schemas[schemaName].validate(data);
    if (error) {
      throw new Error(`Validation failed for schema '${schemaName}': ${error.message}`);
    }

    return value;
  }

  // Utility methods
  isValidMessage(message) {
    try {
      this.validateMongoDocument(message);
      return true;
    } catch (error) {
      return false;
    }
  }

  compareMessages(message1, message2) {
    // Compare by offset within same topic/partition
    if (message1.topic !== message2.topic) {
      return message1.topic.localeCompare(message2.topic);
    }
    
    if (message1.partition !== message2.partition) {
      return message1.partition - message2.partition;
    }
    
    return parseInt(message1.offset) - parseInt(message2.offset);
  }

  cloneMessage(message) {
    return JSON.parse(JSON.stringify(message));
  }
}

// Export singleton instance
module.exports = new MessageModel();