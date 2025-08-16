const { kafka } = require('kafkajs');
const config = require('./index');
const logger = require('../utils/logger');

class KafkaConfig {
  constructor() {
    this.kafka = null;
    this.consumer = null;
    this.producer = null;
    this.admin = null;
  }

  getKafkaClient() {
    if (!this.kafka) {
      this.kafka = kafka({
        clientId: config.kafka.clientId,
        brokers: config.kafka.brokers,
        connectionTimeout: config.kafka.connectionTimeout,
        requestTimeout: config.kafka.requestTimeout,
        retry: {
          initialRetryTime: 100,
          retries: config.app.maxRetries
        },
        logLevel: this.getKafkaLogLevel(),
        logCreator: this.createKafkaLogger()
      });
    }
    return this.kafka;
  }

  getConsumer() {
    if (!this.consumer) {
      const kafka = this.getKafkaClient();
      this.consumer = kafka.consumer({
        groupId: config.kafka.groupId,
        sessionTimeout: 30000,
        rebalanceTimeout: 60000,
        heartbeatInterval: 3000,
        metadataMaxAge: 300000,
        allowAutoTopicCreation: false,
        maxBytesPerPartition: 1048576,
        minBytes: config.kafka.consumer.fetchMinBytes,
        maxBytes: 1048576,
        maxWaitTimeInMs: config.kafka.consumer.fetchMaxWaitMs,
        retry: {
          initialRetryTime: 100,
          retries: config.app.maxRetries
        }
      });
    }
    return this.consumer;
  }

  getProducer() {
    if (!this.producer) {
      const kafka = this.getKafkaClient();
      this.producer = kafka.producer({
        maxInFlightRequests: 1,
        idempotent: true,
        transactionTimeout: 30000,
        retry: {
          initialRetryTime: 100,
          retries: config.app.maxRetries
        }
      });
    }
    return this.producer;
  }

  getAdmin() {
    if (!this.admin) {
      const kafka = this.getKafkaClient();
      this.admin = kafka.admin();
    }
    return this.admin;
  }

  getKafkaLogLevel() {
    const logLevelMap = {
      'debug': 4, // NOTHING
      'info': 2,  // WARN
      'warn': 1,  // ERROR
      'error': 0  // ERROR
    };
    return logLevelMap[config.app.logLevel] || 2;
  }

  createKafkaLogger() {
    return ({ level, log }) => {
      const { message, ...extra } = log;
      
      switch (level) {
        case 0: // ERROR
          logger.error(message, extra);
          break;
        case 1: // WARN
          logger.warn(message, extra);
          break;
        case 2: // INFO
          logger.info(message, extra);
          break;
        case 4: // DEBUG
          logger.debug(message, extra);
          break;
        default:
          logger.info(message, extra);
      }
    };
  }

  async createTopics(topics) {
    const admin = this.getAdmin();
    try {
      await admin.connect();
      
      const existingTopics = await admin.listTopics();
      const topicsToCreate = topics.filter(topic => 
        !existingTopics.includes(topic.topic)
      );

      if (topicsToCreate.length > 0) {
        await admin.createTopics({
          topics: topicsToCreate,
          waitForLeaders: true
        });
        logger.info('Topics created successfully', { topics: topicsToCreate });
      } else {
        logger.info('All topics already exist');
      }
    } catch (error) {
      logger.error('Failed to create topics', { error: error.message });
      throw error;
    } finally {
      await admin.disconnect();
    }
  }

  async getTopicMetadata(topicName) {
    const admin = this.getAdmin();
    try {
      await admin.connect();
      const metadata = await admin.fetchTopicMetadata({ topics: [topicName] });
      return metadata.topics[0];
    } catch (error) {
      logger.error('Failed to fetch topic metadata', { 
        topic: topicName, 
        error: error.message 
      });
      throw error;
    } finally {
      await admin.disconnect();
    }
  }

  async healthCheck() {
    const admin = this.getAdmin();
    try {
      await admin.connect();
      const metadata = await admin.fetchTopicMetadata({ topics: [] });
      await admin.disconnect();
      return {
        status: 'healthy',
        brokers: metadata.brokers.length,
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

module.exports = new KafkaConfig();