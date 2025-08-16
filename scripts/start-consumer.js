#!/usr/bin/env node

const app = require('../src/app');
const logger = require('../src/utils/logger');

async function startConsumer() {
  try {
    logger.info('Starting Kafka to MongoDB consumer...');
    await app.start();
  } catch (error) {
    logger.error('Failed to start consumer', { 
      error: error.message,
      stack: error.stack
    });
    process.exit(1);
  }
}

// Start the consumer
startConsumer();