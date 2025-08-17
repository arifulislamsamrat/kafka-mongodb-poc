const mongoConfig = require('../config/mongodb');
const config = require('../config');
const logger = require('../utils/logger');
const errorHandler = require('../utils/errorHandler');
const validator = require('../utils/validator');

class MongoDBService {
  constructor() {
    this.isConnected = false;
    this.insertCount = 0;
    this.updateCount = 0;
    this.deleteCount = 0;
    this.errorCount = 0;
    this.startTime = Date.now();
    this.circuitBreaker = errorHandler.createCircuitBreaker('mongodb');
    this.operationStats = {
      totalOperationTime: 0,
      averageOperationTime: 0,
      maxOperationTime: 0,
      minOperationTime: Infinity,
      operationCounts: {
        insert: 0,
        update: 0,
        delete: 0,
        find: 0,
        aggregate: 0
      }
    };
    this.connectionPool = {
      currentConnections: 0,
      maxConnections: config.mongodb.options.maxPoolSize,
      availableConnections: 0
    };
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
    const startTime = Date.now();
    
    try {
      if (!this.isConnected) {
        await this.initialize();
      }

      // Validate and enrich the message
      const document = validator.enrichMessage(message);
      
      const result = await this.circuitBreaker.execute(async () => {
        const collection = mongoConfig.getCollection();
        return await collection.insertOne(document, {
          writeConcern: { w: 'majority', j: true }
        });
      });

      this.insertCount++;
      this.operationStats.operationCounts.insert++;
      
      const operationTime = Date.now() - startTime;
      this.updateOperationStats(operationTime, 'insert');

      logger.debug('Message inserted successfully', {
        messageId: document.id,
        insertedId: result.insertedId,
        partition: document.partition,
        offset: document.offset,
        operationTime: `${operationTime}ms`
      });

      return result;
    } catch (error) {
      const operationTime = Date.now() - startTime;
      this.errorCount++;
      
      // Handle duplicate key errors specially
      if (error.code === 11000) {
        logger.warn('Duplicate message detected, skipping', {
          messageId: validator.generateMessageId(message),
          error: error.message,
          operationTime: `${operationTime}ms`
        });
        return { insertedId: null, skipped: true };
      }

      const retryInfo = errorHandler.handleMongoError(error, {
        operation: 'insertMessage',
        messageId: validator.generateMessageId(message),
        operationTime
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
        error: error.message,
        operationTime: `${operationTime}ms`
      });
      throw error;
    }
  }

  async insertBatch(messages) {
    const startTime = Date.now();
    
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
      this.operationStats.operationCounts.insert += result.insertedCount;
      
      const operationTime = Date.now() - startTime;
      this.updateOperationStats(operationTime, 'insertBatch');

      logger.info('Batch inserted successfully', {
        batchSize: documents.length,
        insertedCount: result.insertedCount,
        duplicateErrors: documents.length - result.insertedCount,
        operationTime: `${operationTime}ms`,
        avgTimePerDoc: `${(operationTime / documents.length).toFixed(2)}ms`
      });

      return result;
    } catch (error) {
      const operationTime = Date.now() - startTime;
      
      // Handle bulk write errors
      if (error.code === 11000 || error.name === 'BulkWriteError') {
        const insertedCount = error.result?.insertedCount || 0;
        const duplicateCount = messages.length - insertedCount;
        
        this.insertCount += insertedCount;
        this.operationStats.operationCounts.insert += insertedCount;
        
        logger.warn('Batch insert completed with duplicates', {
          batchSize: messages.length,
          insertedCount,
          duplicateCount,
          error: error.message,
          operationTime: `${operationTime}ms`
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
        batchSize: messages.length,
        operationTime
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
        error: error.message,
        operationTime: `${operationTime}ms`
      });
      throw error;
    }
  }

  async findMessage(query, options = {}) {
    const startTime = Date.now();
    
    try {
      const collection = mongoConfig.getCollection();
      
      const cursor = collection.find(query, {
        limit: options.limit || 100,
        skip: options.skip || 0,
        sort: options.sort || { receivedAt: -1 },
        projection: options.projection
      });

      const documents = await cursor.toArray();
      
      const operationTime = Date.now() - startTime;
      this.operationStats.operationCounts.find++;
      this.updateOperationStats(operationTime, 'find');

      logger.debug('Messages found', {
        query: JSON.stringify(query),
        count: documents.length,
        limit: options.limit,
        operationTime: `${operationTime}ms`
      });

      return documents;
    } catch (error) {
      const operationTime = Date.now() - startTime;
      this.errorCount++;
      
      logger.error('Failed to find messages', {
        query: JSON.stringify(query),
        error: error.message,
        operationTime: `${operationTime}ms`
      });
      throw error;
    }
  }

  async findMessageById(messageId) {
    const startTime = Date.now();
    
    try {
      const collection = mongoConfig.getCollection();
      const document = await collection.findOne({ id: messageId });
      
      const operationTime = Date.now() - startTime;
      this.operationStats.operationCounts.find++;
      this.updateOperationStats(operationTime, 'findById');

      if (document) {
        logger.debug('Message found by ID', { 
          messageId,
          operationTime: `${operationTime}ms`
        });
      } else {
        logger.debug('Message not found by ID', { 
          messageId,
          operationTime: `${operationTime}ms`
        });
      }

      return document;
    } catch (error) {
      const operationTime = Date.now() - startTime;
      this.errorCount++;
      
      logger.error('Failed to find message by ID', {
        messageId,
        error: error.message,
        operationTime: `${operationTime}ms`
      });
      throw error;
    }
  }

  async updateMessage(messageId, update) {
    const startTime = Date.now();
    
    try {
      const collection = mongoConfig.getCollection();
      
      const result = await collection.updateOne(
        { id: messageId },
        { 
          $set: { 
            ...update, 
            updatedAt: new Date(),
            updatedBy: 'system'
          } 
        }
      );

      this.updateCount++;
      this.operationStats.operationCounts.update++;
      
      const operationTime = Date.now() - startTime;
      this.updateOperationStats(operationTime, 'update');

      logger.debug('Message updated', {
        messageId,
        matchedCount: result.matchedCount,
        modifiedCount: result.modifiedCount,
        operationTime: `${operationTime}ms`
      });

      return result;
    } catch (error) {
      const operationTime = Date.now() - startTime;
      this.errorCount++;
      
      logger.error('Failed to update message', {
        messageId,
        error: error.message,
        operationTime: `${operationTime}ms`
      });
      throw error;
    }
  }

  async updateMessages(filter, update) {
    const startTime = Date.now();
    
    try {
      const collection = mongoConfig.getCollection();
      
      const result = await collection.updateMany(
        filter,
        { 
          $set: { 
            ...update, 
            updatedAt: new Date(),
            updatedBy: 'system'
          } 
        }
      );

      this.updateCount += result.modifiedCount;
      this.operationStats.operationCounts.update += result.modifiedCount;
      
      const operationTime = Date.now() - startTime;
      this.updateOperationStats(operationTime, 'updateMany');

      logger.info('Messages updated', {
        filter: JSON.stringify(filter),
        matchedCount: result.matchedCount,
        modifiedCount: result.modifiedCount,
        operationTime: `${operationTime}ms`
      });

      return result;
    } catch (error) {
      const operationTime = Date.now() - startTime;
      this.errorCount++;
      
      logger.error('Failed to update messages', {
        filter: JSON.stringify(filter),
        error: error.message,
        operationTime: `${operationTime}ms`
      });
      throw error;
    }
  }

  async deleteMessage(messageId) {
    const startTime = Date.now();
    
    try {
      const collection = mongoConfig.getCollection();
      const result = await collection.deleteOne({ id: messageId });
      
      this.deleteCount += result.deletedCount;
      this.operationStats.operationCounts.delete += result.deletedCount;
      
      const operationTime = Date.now() - startTime;
      this.updateOperationStats(operationTime, 'delete');

      logger.debug('Message deleted', {
        messageId,
        deletedCount: result.deletedCount,
        operationTime: `${operationTime}ms`
      });

      return result;
    } catch (error) {
      const operationTime = Date.now() - startTime;
      this.errorCount++;
      
      logger.error('Failed to delete message', {
        messageId,
        error: error.message,
        operationTime: `${operationTime}ms`
      });
      throw error;
    }
  }

  async deleteMessages(filter) {
    const startTime = Date.now();
    
    try {
      const collection = mongoConfig.getCollection();
      const result = await collection.deleteMany(filter);
      
      this.deleteCount += result.deletedCount;
      this.operationStats.operationCounts.delete += result.deletedCount;
      
      const operationTime = Date.now() - startTime;
      this.updateOperationStats(operationTime, 'deleteMany');

      logger.info('Messages deleted', {
        filter: JSON.stringify(filter),
        deletedCount: result.deletedCount,
        operationTime: `${operationTime}ms`
      });

      return result;
    } catch (error) {
      const operationTime = Date.now() - startTime;
      this.errorCount++;
      
      logger.error('Failed to delete messages', {
        filter: JSON.stringify(filter),
        error: error.message,
        operationTime: `${operationTime}ms`
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

  async getMessagesByTopic(topic, options = {}) {
    try {
      const query = { topic: topic };
      return await this.findMessage(query, options);
    } catch (error) {
      logger.error('Failed to get messages by topic', {
        topic,
        error: error.message
      });
      throw error;
    }
  }

  async getMessagesByPriority(priority, options = {}) {
    try {
      const query = { priority: priority };
      return await this.findMessage(query, options);
    } catch (error) {
      logger.error('Failed to get messages by priority', {
        priority,
        error: error.message
      });
      throw error;
    }
  }

  async aggregateMessages(pipeline) {
    const startTime = Date.now();
    
    try {
      const collection = mongoConfig.getCollection();
      const cursor = collection.aggregate(pipeline);
      const results = await cursor.toArray();
      
      const operationTime = Date.now() - startTime;
      this.operationStats.operationCounts.aggregate++;
      this.updateOperationStats(operationTime, 'aggregate');

      logger.debug('Aggregation completed', {
        pipelineStages: pipeline.length,
        resultCount: results.length,
        operationTime: `${operationTime}ms`
      });

      return results;
    } catch (error) {
      const operationTime = Date.now() - startTime;
      this.errorCount++;
      
      logger.error('Failed to aggregate messages', {
        pipeline: JSON.stringify(pipeline),
        error: error.message,
        operationTime: `${operationTime}ms`
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
            messageTypes: { $addToSet: '$messageType' },
            priorities: { $addToSet: '$priority' },
            topics: { $addToSet: '$topic' },
            actionCounts: {
              $push: {
                $cond: [
                  { $ne: ['$value.action', null] },
                  '$value.action',
                  '$REMOVE'
                ]
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
            messageTypes: 1,
            priorities: 1,
            topics: 1,
            actionCounts: 1
          }
        }
      ];

      const stats = await this.aggregateMessages(pipeline);
      return stats[0] || {};
    } catch (error) {
      logger.error('Failed to get message statistics', {
        error: error.message
      });
      throw error;
    }
  }

  async getHourlyStats(hours = 24) {
    try {
      const startDate = new Date();
      startDate.setHours(startDate.getHours() - hours);

      const pipeline = [
        {
          $match: {
            timestamp: { $gte: startDate }
          }
        },
        {
          $group: {
            _id: {
              hour: { $hour: '$timestamp' },
              date: { $dateToString: { format: '%Y-%m-%d', date: '$timestamp' } }
            },
            messageCount: { $sum: 1 },
            uniqueUsers: { $addToSet: '$value.userId' },
            errorCount: {
              $sum: {
                $cond: [{ $eq: ['$validationStatus', 'error'] }, 1, 0]
              }
            }
          }
        },
        {
          $project: {
            _id: 0,
            hour: '$_id.hour',
            date: '$_id.date',
            messageCount: 1,
            uniqueUserCount: { $size: '$uniqueUsers' },
            errorCount: 1
          }
        },
        {
          $sort: { date: 1, hour: 1 }
        }
      ];

      return await this.aggregateMessages(pipeline);
    } catch (error) {
      logger.error('Failed to get hourly statistics', {
        error: error.message
      });
      throw error;
    }
  }

  async getUserActivityStats(userId, days = 7) {
    try {
      const startDate = new Date();
      startDate.setDate(startDate.getDate() - days);

      const pipeline = [
        {
          $match: {
            'value.userId': userId,
            timestamp: { $gte: startDate }
          }
        },
        {
          $group: {
            _id: '$value.action',
            count: { $sum: 1 },
            lastOccurrence: { $max: '$timestamp' },
            firstOccurrence: { $min: '$timestamp' }
          }
        },
        {
          $sort: { count: -1 }
        }
      ];

      return await this.aggregateMessages(pipeline);
    } catch (error) {
      logger.error('Failed to get user activity statistics', {
        userId,
        error: error.message
      });
      throw error;
    }
  }

  async cleanupOldMessages(retentionDays = 30) {
    try {
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - retentionDays);

      const result = await this.deleteMessages({
        timestamp: { $lt: cutoffDate }
      });

      logger.info('Old messages cleaned up', {
        retentionDays,
        cutoffDate,
        deletedCount: result.deletedCount
      });

      return result;
    } catch (error) {
      logger.error('Failed to cleanup old messages', {
        retentionDays,
        error: error.message
      });
      throw error;
    }
  }

  async archiveOldMessages(archiveCollectionName, retentionDays = 90) {
    const session = mongoConfig.getClient().startSession();
    
    try {
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - retentionDays);

      const result = await session.withTransaction(async () => {
        const db = mongoConfig.getDb();
        const sourceCollection = mongoConfig.getCollection();
        const archiveCollection = db.collection(archiveCollectionName);

        // Find messages to archive
        const messagesToArchive = await sourceCollection.find({
          timestamp: { $lt: cutoffDate }
        }).toArray();

        if (messagesToArchive.length === 0) {
          return { archivedCount: 0, deletedCount: 0 };
        }

        // Insert into archive collection
        await archiveCollection.insertMany(messagesToArchive, { session });

        // Delete from main collection
        const deleteResult = await sourceCollection.deleteMany({
          timestamp: { $lt: cutoffDate }
        }, { session });

        return {
          archivedCount: messagesToArchive.length,
          deletedCount: deleteResult.deletedCount
        };
      });

      logger.info('Messages archived successfully', {
        retentionDays,
        cutoffDate,
        archivedCount: result.archivedCount,
        deletedCount: result.deletedCount
      });

      return result;
    } catch (error) {
      logger.error('Failed to archive old messages', {
        retentionDays,
        error: error.message
      });
      throw error;
    } finally {
      await session.endSession();
    }
  }

  async createIndexes(indexes) {
    try {
      const collection = mongoConfig.getCollection();
      const results = [];

      for (const index of indexes) {
        try {
          const result = await collection.createIndex(index.key, index.options || {});
          results.push({ index: index.name || JSON.stringify(index.key), result });
          logger.debug('Index created', { index: index.name || JSON.stringify(index.key) });
        } catch (error) {
          if (error.code === 85) {
            logger.info('Index already exists', { index: index.name });
            results.push({ index: index.name, result: 'already_exists' });
          } else {
            throw error;
          }
        }
      }

      return results;
    } catch (error) {
      logger.error('Failed to create indexes', {
        error: error.message
      });
      throw error;
    }
  }

  async getCollectionInfo() {
    try {
      const stats = await mongoConfig.getCollectionStats();
      const collection = mongoConfig.getCollection();
      const indexes = await collection.indexes();
      
      return {
        stats,
        indexes: indexes.length,
        indexDetails: indexes
      };
    } catch (error) {
      logger.error('Failed to get collection info', {
        error: error.message
      });
      throw error;
    }
  }

  async disconnect() {
    try {
      if (this.isConnected) {
        logger.mongoLog('Disconnecting MongoDB service');
        await mongoConfig.disconnect();
        this.isConnected = false;
        logger.mongoLog('MongoDB service disconnected successfully');
      }
    } catch (error) {
      logger.error('Error disconnecting MongoDB service', { 
        error: error.message 
      });
      throw error;
    }
  }

  updateOperationStats(operationTime, operationType) {
    this.operationStats.totalOperationTime += operationTime;
    
    const totalOperations = Object.values(this.operationStats.operationCounts)
      .reduce((sum, count) => sum + count, 0);
    
    this.operationStats.averageOperationTime = 
      this.operationStats.totalOperationTime / totalOperations;
    
    if (operationTime > this.operationStats.maxOperationTime) {
      this.operationStats.maxOperationTime = operationTime;
    }
    
    if (operationTime < this.operationStats.minOperationTime) {
      this.operationStats.minOperationTime = operationTime;
    }
  }

  getMetrics() {
    const uptime = Date.now() - this.startTime;
    const totalOperations = this.insertCount + this.updateCount + this.deleteCount;
    const operationsPerSecond = totalOperations > 0 ? 
      (totalOperations / (uptime / 1000)).toFixed(2) : 0;

    return {
      isConnected: this.isConnected,
      insertCount: this.insertCount,
      updateCount: this.updateCount,
      deleteCount: this.deleteCount,
      errorCount: this.errorCount,
      totalOperations: totalOperations,
      uptime: uptime,
      operationsPerSecond: parseFloat(operationsPerSecond),
      circuitBreakerState: this.circuitBreaker.getState(),
      successRate: totalOperations > 0 ? 
        ((totalOperations - this.errorCount) / totalOperations * 100).toFixed(2) : 0,
      operationStats: {
        ...this.operationStats,
        averageOperationTime: parseFloat(this.operationStats.averageOperationTime.toFixed(2)),
        maxOperationTime: this.operationStats.maxOperationTime,
        minOperationTime: this.operationStats.minOperationTime === Infinity ? 0 : this.operationStats.minOperationTime
      },
      connectionPool: this.connectionPool
    };
  }

  async healthCheck() {
    try {
      const metrics = this.getMetrics();
      const mongoHealth = await mongoConfig.healthCheck();
      
      return {
        status: this.isConnected && mongoHealth.status === 'healthy' ? 'healthy' : 'unhealthy',
        mongodb: {
          connected: this.isConnected,
          insertCount: this.insertCount,
          updateCount: this.updateCount,
          deleteCount: this.deleteCount,
          errorCount: this.errorCount,
          successRate: metrics.successRate,
          operationsPerSecond: metrics.operationsPerSecond,
          averageOperationTime: metrics.operationStats.averageOperationTime
        },
        database: mongoHealth,
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

  // Transaction support for complex operations
  async withTransaction(operations) {
    const session = mongoConfig.getClient().startSession();
    
    try {
      const result = await session.withTransaction(async () => {
        return await operations(session);
      });

      logger.debug('Transaction completed successfully');
      return result;
    } catch (error) {
      logger.error('Transaction failed', { error: error.message });
      throw error;
    } finally {
      await session.endSession();
    }
  }

  // Bulk operations for better performance
  async bulkWrite(operations) {
    const startTime = Date.now();
    
    try {
      const collection = mongoConfig.getCollection();
      const result = await collection.bulkWrite(operations, {
        ordered: false,
        writeConcern: { w: 'majority', j: true }
      });

      const operationTime = Date.now() - startTime;
      
      // Update counts based on operation types
      this.insertCount += result.insertedCount || 0;
      this.updateCount += (result.modifiedCount || 0) + (result.upsertedCount || 0);
      this.deleteCount += result.deletedCount || 0;

      this.updateOperationStats(operationTime, 'bulkWrite');

      logger.info('Bulk write completed', {
        operationCount: operations.length,
        insertedCount: result.insertedCount,
        modifiedCount: result.modifiedCount,
        deletedCount: result.deletedCount,
        upsertedCount: result.upsertedCount,
        operationTime: `${operationTime}ms`
      });

      return result;
    } catch (error) {
      const operationTime = Date.now() - startTime;
      this.errorCount++;
      
      logger.error('Bulk write failed', {
        operationCount: operations.length,
        error: error.message,
        operationTime: `${operationTime}ms`
      });
      throw error;
    }
  }

  async createTextIndex(fields, options = {}) {
    try {
      const collection = mongoConfig.getCollection();
      const indexSpec = {};
      
      fields.forEach(field => {
        indexSpec[field] = 'text';
      });

      const result = await collection.createIndex(indexSpec, {
        name: `text_index_${fields.join('_')}`,
        ...options
      });

      logger.info('Text index created', {
        fields,
        indexName: result
      });

      return result;
    } catch (error) {
      logger.error('Failed to create text index', {
        fields,
        error: error.message
      });
      throw error;
    }
  }

  async searchMessages(searchText, options = {}) {
    try {
      const query = {
        $text: { $search: searchText }
      };

      // Add additional filters if provided
      if (options.filter) {
        Object.assign(query, options.filter);
      }

      const searchOptions = {
        ...options,
        projection: {
          ...options.projection,
          score: { $meta: 'textScore' }
        },
        sort: { score: { $meta: 'textScore' } }
      };

      return await this.findMessage(query, searchOptions);
    } catch (error) {
      logger.error('Failed to search messages', {
        searchText,
        error: error.message
      });
      throw error;
    }
  }

  resetMetrics() {
    this.insertCount = 0;
    this.updateCount = 0;
    this.deleteCount = 0;
    this.errorCount = 0;
    this.startTime = Date.now();
    this.operationStats = {
      totalOperationTime: 0,
      averageOperationTime: 0,
      maxOperationTime: 0,
      minOperationTime: Infinity,
      operationCounts: {
        insert: 0,
        update: 0,
        delete: 0,
        find: 0,
        aggregate: 0
      }
    };
    
    logger.mongoLog('MongoDB service metrics reset');
  }
}

module.exports = MongoDBService;