require('dotenv').config();
const Joi = require('joi');

// Configuration validation schema
const configSchema = Joi.object({
  NODE_ENV: Joi.string()
    .valid('development', 'production', 'test')
    .default('development'),
  
  // Kafka Configuration
  KAFKA_BROKERS: Joi.string().required(),
  KAFKA_CLIENT_ID: Joi.string().default('kafka-mongodb-poc'),
  KAFKA_GROUP_ID: Joi.string().default('mongodb-consumer-group'),
  KAFKA_TOPIC: Joi.string().default('user-events'),
  KAFKA_CONNECTION_TIMEOUT: Joi.number().default(3000),
  KAFKA_REQUEST_TIMEOUT: Joi.number().default(30000),
  
  // MongoDB Configuration
  MONGODB_URL: Joi.string().required(),
  MONGODB_DB_NAME: Joi.string().default('kafka_data'),
  MONGODB_COLLECTION_NAME: Joi.string().default('events'),
  MONGODB_MAX_POOL_SIZE: Joi.number().default(10),
  MONGODB_MIN_POOL_SIZE: Joi.number().default(5),
  
  // Application Configuration
  LOG_LEVEL: Joi.string()
    .valid('error', 'warn', 'info', 'debug')
    .default('info'),
  BATCH_SIZE: Joi.number().default(100),
  BATCH_TIMEOUT: Joi.number().default(5000),
  MAX_RETRIES: Joi.number().default(3),
  RETRY_DELAY: Joi.number().default(1000),
  
  // Health Check & Monitoring
  HEALTH_CHECK_PORT: Joi.number().default(3001),
  METRICS_PORT: Joi.number().default(3002),
  ENABLE_HEALTH_CHECK: Joi.boolean().default(true),
  ENABLE_METRICS: Joi.boolean().default(true),
  
  // Error Handling
  ENABLE_DLQ: Joi.boolean().default(false),
  DLQ_TOPIC: Joi.string().default('user-events-dlq'),
  
  // Performance Tuning
  CONSUMER_PARTITION_CONCURRENCY: Joi.number().default(1),
  CONSUMER_FETCH_MIN_BYTES: Joi.number().default(1),
  CONSUMER_FETCH_MAX_WAIT_MS: Joi.number().default(5000)
}).unknown();

const { error, value: envVars } = configSchema.validate(process.env);

if (error) {
  throw new Error(`Config validation error: ${error.message}`);
}

const config = {
  env: envVars.NODE_ENV,
  
  kafka: {
    brokers: envVars.KAFKA_BROKERS.split(','),
    clientId: envVars.KAFKA_CLIENT_ID,
    groupId: envVars.KAFKA_GROUP_ID,
    topic: envVars.KAFKA_TOPIC,
    connectionTimeout: envVars.KAFKA_CONNECTION_TIMEOUT,
    requestTimeout: envVars.KAFKA_REQUEST_TIMEOUT,
    consumer: {
      partitionConcurrency: envVars.CONSUMER_PARTITION_CONCURRENCY,
      fetchMinBytes: envVars.CONSUMER_FETCH_MIN_BYTES,
      fetchMaxWaitMs: envVars.CONSUMER_FETCH_MAX_WAIT_MS
    }
  },
  
  mongodb: {
    url: envVars.MONGODB_URL,
    dbName: envVars.MONGODB_DB_NAME,
    collectionName: envVars.MONGODB_COLLECTION_NAME,
    options: {
      maxPoolSize: envVars.MONGODB_MAX_POOL_SIZE,
      minPoolSize: envVars.MONGODB_MIN_POOL_SIZE,
      maxIdleTimeMS: 30000,
      serverSelectionTimeoutMS: 5000,
      socketTimeoutMS: 45000
    }
  },
  
  app: {
    logLevel: envVars.LOG_LEVEL,
    batchSize: envVars.BATCH_SIZE,
    batchTimeout: envVars.BATCH_TIMEOUT,
    maxRetries: envVars.MAX_RETRIES,
    retryDelay: envVars.RETRY_DELAY
  },
  
  monitoring: {
    healthCheckPort: envVars.HEALTH_CHECK_PORT,
    metricsPort: envVars.METRICS_PORT,
    enableHealthCheck: envVars.ENABLE_HEALTH_CHECK,
    enableMetrics: envVars.ENABLE_METRICS
  },
  
  errorHandling: {
    enableDLQ: envVars.ENABLE_DLQ,
    dlqTopic: envVars.DLQ_TOPIC
  }
};

module.exports = config;