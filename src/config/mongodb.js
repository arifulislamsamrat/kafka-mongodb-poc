const { MongoClient } = require('mongodb');
const config = require('./index');
const logger = require('../utils/logger');

class MongoDBConfig {
  constructor() {
    this.client = null;
    this.db = null;
    this.collection = null;
    this.isConnected = false;
  }

  async connect() {
    if (this.isConnected) {
      return this.client;
    }

    try {
      logger.info('Connecting to MongoDB...', { url: config.mongodb.url });
      
      this.client = new MongoClient(config.mongodb.url, {
        ...config.mongodb.options,
        useUnifiedTopology: true
      });

      await this.client.connect();
      
      // Test the connection
      await this.client.db('admin').command({ ping: 1 });
      
      this.db = this.client.db(config.mongodb.dbName);
      this.collection = this.db.collection(config.mongodb.collectionName);
      this.isConnected = true;

      logger.info('MongoDB connected successfully', {
        database: config.mongodb.dbName,
        collection: config.mongodb.collectionName
      });

      // Set up connection event listeners
      this.setupEventListeners();

      return this.client;
    } catch (error) {
      logger.error('Failed to connect to MongoDB', { 
        error: error.message,
        url: config.mongodb.url 
      });
      throw error;
    }
  }

  setupEventListeners() {
    if (!this.client) return;

    this.client.on('serverOpening', () => {
      logger.info('MongoDB server opening');
    });

    this.client.on('serverClosed', () => {
      logger.warn('MongoDB server closed');
      this.isConnected = false;
    });

    this.client.on('error', (error) => {
      logger.error('MongoDB client error', { error: error.message });
      this.isConnected = false;
    });

    this.client.on('timeout', () => {
      logger.warn('MongoDB client timeout');
    });
  }

  async disconnect() {
    if (this.client && this.isConnected) {
      try {
        await this.client.close();
        this.isConnected = false;
        this.client = null;
        this.db = null;
        this.collection = null;
        logger.info('MongoDB disconnected successfully');
      } catch (error) {
        logger.error('Error disconnecting from MongoDB', { 
          error: error.message 
        });
        throw error;
      }
    }
  }

  getClient() {
    if (!this.isConnected || !this.client) {
      throw new Error('MongoDB client is not connected');
    }
    return this.client;
  }

  getDb() {
    if (!this.isConnected || !this.db) {
      throw new Error('MongoDB database is not connected');
    }
    return this.db;
  }

  getCollection(collectionName = null) {
    if (!this.isConnected) {
      throw new Error('MongoDB is not connected');
    }
    
    if (collectionName) {
      return this.db.collection(collectionName);
    }
    
    if (!this.collection) {
      throw new Error('Default collection is not initialized');
    }
    
    return this.collection;
  }

  async setupIndexes() {
    try {
      const collection = this.getCollection();
      
      // Create indexes for better query performance
      const indexes = [
        // Index on timestamp for time-based queries
        { key: { timestamp: 1 }, name: 'timestamp_1' },
        // Index on receivedAt for processing order
        { key: { receivedAt: 1 }, name: 'receivedAt_1' },
        // Compound index on partition and offset for uniqueness
        { 
          key: { partition: 1, offset: 1 }, 
          name: 'partition_offset_1',
          unique: true 
        },
        // Index on message key for efficient lookups
        { key: { key: 1 }, name: 'key_1' },
        // Index on value fields that might be queried frequently
        { key: { 'value.userId': 1 }, name: 'value_userId_1' },
        { key: { 'value.action': 1 }, name: 'value_action_1' }
      ];

      for (const index of indexes) {
        try {
          await collection.createIndex(index.key, {
            name: index.name,
            unique: index.unique || false,
            background: true
          });
          logger.info('Index created', { index: index.name });
        } catch (error) {
          if (error.code === 85) {
            // Index already exists with different options
            logger.info('Index already exists', { index: index.name });
          } else {
            throw error;
          }
        }
      }

      logger.info('All indexes created successfully');
    } catch (error) {
      logger.error('Failed to setup indexes', { error: error.message });
      throw error;
    }
  }

  async setupCollectionValidation() {
    try {
      const db = this.getDb();
      
      // Define schema validation for the collection
      const validationSchema = {
        $jsonSchema: {
          bsonType: 'object',
          required: ['partition', 'offset', 'timestamp', 'value', 'receivedAt'],
          properties: {
            partition: { bsonType: 'int' },
            offset: { bsonType: 'string' },
            timestamp: { bsonType: 'date' },
            key: { bsonType: ['string', 'null'] },
            value: { bsonType: 'object' },
            headers: { bsonType: ['object', 'null'] },
            receivedAt: { bsonType: 'date' }
          }
        }
      };

      await db.command({
        collMod: config.mongodb.collectionName,
        validator: validationSchema,
        validationLevel: 'moderate',
        validationAction: 'warn'
      });

      logger.info('Collection validation schema applied');
    } catch (error) {
      logger.error('Failed to setup collection validation', { 
        error: error.message 
      });
      // Don't throw error as this is optional
    }
  }

  async healthCheck() {
    try {
      if (!this.isConnected) {
        return {
          status: 'disconnected',
          timestamp: new Date().toISOString()
        };
      }

      const client = this.getClient();
      await client.db('admin').command({ ping: 1 });
      
      const stats = await this.db.stats();
      
      return {
        status: 'healthy',
        database: config.mongodb.dbName,
        collections: stats.collections,
        dataSize: stats.dataSize,
        storageSize: stats.storageSize,
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

  async getCollectionStats() {
    try {
      const collection = this.getCollection();
      const stats = await collection.stats();
      return {
        count: stats.count,
        size: stats.size,
        avgObjSize: stats.avgObjSize,
        storageSize: stats.storageSize,
        indexes: stats.nindexes,
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      logger.error('Failed to get collection stats', { error: error.message });
      throw error;
    }
  }
}

module.exports = new MongoDBConfig();