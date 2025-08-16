const winston = require('winston');
const config = require('../config');

// Custom format for console output
const consoleFormat = winston.format.combine(
  winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
  winston.format.errors({ stack: true }),
  winston.format.colorize(),
  winston.format.printf(({ level, message, timestamp, ...meta }) => {
    const metaStr = Object.keys(meta).length ? JSON.stringify(meta, null, 2) : '';
    return `${timestamp} [${level}]: ${message} ${metaStr}`;
  })
);

// Custom format for file output
const fileFormat = winston.format.combine(
  winston.format.timestamp(),
  winston.format.errors({ stack: true }),
  winston.format.json()
);

// Create transports
const transports = [
  new winston.transports.Console({
    level: config.app.logLevel,
    format: consoleFormat,
    handleExceptions: true,
    handleRejections: true
  })
];

// Add file transport in production
if (config.env === 'production') {
  transports.push(
    new winston.transports.File({
      filename: 'logs/error.log',
      level: 'error',
      format: fileFormat,
      maxsize: 5242880, // 5MB
      maxFiles: 5
    }),
    new winston.transports.File({
      filename: 'logs/combined.log',
      format: fileFormat,
      maxsize: 5242880, // 5MB
      maxFiles: 5
    })
  );
}

// Create logger
const logger = winston.createLogger({
  level: config.app.logLevel,
  format: fileFormat,
  defaultMeta: { 
    service: 'kafka-mongodb-poc',
    version: process.env.npm_package_version || '1.0.0'
  },
  transports,
  exceptionHandlers: [
    new winston.transports.Console({
      format: consoleFormat
    })
  ],
  rejectionHandlers: [
    new winston.transports.Console({
      format: consoleFormat
    })
  ],
  exitOnError: false
});

// Stream for Morgan HTTP logger
logger.stream = {
  write: (message) => {
    logger.info(message.trim());
  }
};

// Add custom methods for structured logging
logger.kafkaLog = (action, data = {}) => {
  logger.info(`Kafka: ${action}`, { 
    component: 'kafka',
    action,
    ...data 
  });
};

logger.mongoLog = (action, data = {}) => {
  logger.info(`MongoDB: ${action}`, { 
    component: 'mongodb',
    action,
    ...data 
  });
};

logger.appLog = (action, data = {}) => {
  logger.info(`App: ${action}`, { 
    component: 'application',
    action,
    ...data 
  });
};

logger.metricsLog = (metric, value, data = {}) => {
  logger.info(`Metrics: ${metric}`, { 
    component: 'metrics',
    metric,
    value,
    ...data 
  });
};

module.exports = logger;