const mongoConfig = require('../config/mongodb');
const config = require('../config');
const logger = require('../utils/logger');
const errorHandler = require('../utils/errorHandler');
const validator = require('../utils/validator');

class MongoDBService {
  constructor() {
    this.isConnected = false;
    this.insertCount = 0;
    this.errorCount = 0;
    this.circuitBreaker = errorHandler.createCircuitBreaker('mongodb');
  }

  async initialize() {
    try {
      logger.mongoLog('Initializing MongoDB service');
      await mongoConfig.connect();
      
      // Setup indexes and validation
      await mongoConfig.setupIndexes();
      await mongoConfig.setupCollectionValidation();
      
      this.isConnected = true;
      logger.mongoLog('MongoDB service initialized successfully');
    } catch (error) {
      logger.error('Failed to initialize MongoDB service', { 
        error: error.message 
      });
      throw error;
    }
  }

  async insertMessage(message) {
    try {
      if (!this.isConnected) {
        await this.initialize();
      }

      // Validate and enrich the message
      const document = validator.enrichMessage(message);
      
      const result = await this.circuitBreaker.execute(async () => {
        const collection = mongoConfig.getCollection();
        return await collection.insertOne(document);
      });

      this.insertCount++;
      
      logger.debug('Message inserted successfully', {
        messageId: document.id,
        insertedId: result.insertedId,
        partition: document.partition,
        offset: document.offset
      });

      return result;
    } catch (error) {
      this.errorCount++;
      
      // Handle duplicate key errors specially
      if (error.code === 11000) {
        logger.warn('Duplicate message detected, skipping', {
          messageId: validator.generateMessageId(message),
          error: error.message
        });
        return { insertedId: null, skipped: true };
      }

      const retryInfo = errorHandler.handleMongoError(error, {
        operation: 'insertMessage',
        messageId: validator.generateMessageId(message)
      });

      if (retryInfo.shouldRetry) {
        logger.warn('Retrying message insert', {
          messageId: validator.generateMessageId(message),
          attempt: retryInfo.attempt,
          delay: retryInfo.delay
        });
        
        await errorHandler.sleep(retryInfo.delay);
        return this.insertMessage(message);
      }

      logger.error('Failed to insert message', {
        messageId: validator.generateMessageId(message),
        error: error.message
      });
      throw error;
    }
  }

  async insertBatch(messages) {
    try {
      if (!this.isConnected) {
        await this.initialize();
      }

      if (!Array.isArray(messages) || messages.length === 0) {
        throw new Error('Messages must be a non-empty array');
      }

      // Validate and enrich all messages
      const documents = messages.map(message => validator.enrichMessage(message));
      
      // Validate batch
      validator.validateBatch({
        messages: documents,
        batchId: `batch_${Date.now()}`,
        createdAt: new Date(),
        size: documents.length
      });

      const result = await this.circuitBreaker.execute(async () => {
        const collection = mongoConfig.getCollection();
        return await collection.insertMany(documents, {
          ordered: false, // Continue on individual failures
          writeConcern: { w: 'majority', j: true }
        });
      });

      this.insertCount += result.insertedCount;
      
      logger.info('Batch inserted successfully', {
        batchSize: documents.length,
        insertedCount: result.insertedCount,
        duplicateErrors: documents.length - result.insertedCount
      });

      return result;
    } catch (error) {
      // Handle bulk write errors
      if (error.code === 11000 || error.name === 'BulkWriteError') {
        const insertedCount = error.result?.insertedCount || 0;
        const duplicateCount = messages.length - insertedCount;
        
        this.insertCount += insertedCount;
        
        logger.warn('Batch insert completed with duplicates', {
          batchSize: messages.length,
          insertedCount,
          duplicateCount,
          error: error.message
        });

        return {
          insertedCount,
          insertedIds: error.result?.insertedIds || {},
          duplicateCount
        };
      }

      this.errorCount += messages.length;
      
      const retryInfo = errorHandler.handleMongoError(error, {
        operation: 'insertBatch',
        batchSize: messages.length
      });

      if (retryInfo.shouldRetry) {
        logger.warn('Retrying batch insert', {
          batchSize: messages.length,
          attempt: retryInfo.attempt,
          delay: retryInfo.delay
        });
        
        await errorHandler.sleep(retryInfo.delay);
        return this.insertBatch(messages);
      }

      logger.error('Failed to insert batch', {
        batchSize: messages.length,
        error: error.message
      });
      throw error;
    }
  }

  async findMessage(query, options = {}) {
    try {
      const collection = mongoConfig.getCollection();
      
      const cursor = collection.find(query, {
        limit: options.limit || 100,
        skip: options.skip || 0,
        sort: options.sort || { receivedAt: -1 },
        projection: options.projection
      });

      const documents = await cursor.toArray();
      
      logger.debug('Messages found', {
        query: JSON.stringify(query),
        count: documents.length,
        limit: options.limit
      });

      return documents;
    } catch (error) {
      logger.error('Failed to find messages', {
        query: JSON.stringify(query),
        error: error.message
      });
      throw error;
    }
  }

  async findMessageById(messageId) {
    try {
      const collection = mongoConfig.getCollection();
      const document = await collection.findOne({ id: messageId });
      
      if (document) {
        logger.debug('Message found by ID', { messageId });
      } else {
        logger.debug('Message not found by ID', { messageId });
      }

      return document;
    } catch (error) {
      logger.error('Failed to find message by ID', {
        messageId,
        error: error.message
      });
      throw error;
    }
  }

  async updateMessage(messageId, update) {
    try {
      const collection = mongoConfig.getCollection();
      
      const result = await collection.updateOne(
        { id: messageId },
        { 
          $set: { 
            ...update, 
            updatedAt: new Date() 
          } 
        }
      );

      logger.debug('Message updated', {
        messageId,
        matchedCount: result.matchedCount,
        modifiedCount: result.modifiedCount
      });

      return result;
    } catch (error) {
      logger.error('Failed to update message', {
        messageId,
        error: error.message
      });
      throw error;
    }
  }

  async deleteMessage(messageId) {
    try {
      const collection = mongoConfig.getCollection();
      const result = await collection.deleteOne({ id: messageId });
      
      logger.debug('Message deleted', {
        messageId,
        deletedCount: result.deletedCount
      });

      return result;
    } catch (error) {
      logger.error('Failed to delete message', {
        messageId,
        error: error.message
      });
      throw error;
    }
  }

  async getMessagesByDateRange(startDate, endDate, options = {}) {
    try {
      const query = {
        timestamp: {
          $gte: new Date(startDate),
          $lte: new Date(endDate)
        }
      };

      return await this.findMessage(query, options);
    } catch (error) {
      logger.error('Failed to get messages by date range', {
        startDate,
        endDate,
        error: error.message
      });
      throw error;
    }
  }

  async getMessagesByUserId(userId, options = {}) {
    try {
      const query = { 'value.userId': userId };
      return await this.findMessage(query, options);
    } catch (error) {
      logger.error('Failed to get messages by user ID', {
        userId,
        error: error.message
      });
      throw error;
    }
  }

  async getMessagesByAction(action, options = {}) {
    try {
      const query = { 'value.action': action };
      return await this.findMessage(query, options);
    } catch (error) {
      logger.error('Failed to get messages by action', {
        action,
        error: error.message
      });
      throw error;
    }
  }

  async aggregateMessages(pipeline) {
    try {
      const collection = mongoConfig.getCollection();
      const cursor = collection.aggregate(pipeline);
      const results = await cursor.toArray();
      
      logger.debug('Aggregation completed', {
        pipelineStages: pipeline.length,
        resultCount: results.length
      });

      return results;
    } catch (error) {
      logger.error('Failed to aggregate messages', {
        pipeline: JSON.stringify(pipeline),
        error: error.message
      });
      throw error;
    }
  }

  async getMessageStats() {
    try {
      const pipeline = [
        {
          $group: {
            _id: null,
            totalMessages: { $sum: 1 },
            earliestMessage: { $min: '$timestamp' },
            latestMessage: { $max: '$timestamp' },
            uniqueUsers: { $addToSet: '$value.userId' },
            actionCounts: {
              $push: {
                action: '$value.action',
                count: 1
              }
            }
          }
        },
        {
          $project: {
            _id: 0,
            totalMessages: 1,
            earliestMessage: 1,
            latestMessage: 1,
            uniqueUserCount: { $size: '$uniqueUsers' },
            actionCounts: 1
          }
        }
      ];

      const stats = await this.aggregateMessages(pipeline);
      return stats[0] || {};
    } catch (error) {
      logger.error('Failed to get message statistics', {
        error: error.