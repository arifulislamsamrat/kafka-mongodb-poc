const logger = require('../utils/logger');
const validator = require('../utils/validator');
const errorHandler = require('../utils/errorHandler');

class MessageProcessor {
  constructor(mongodbService) {
    this.mongodbService = mongodbService;
    this.processedCount = 0;
    this.errorCount = 0;
  }

  async processMessage(message) {
    try {
      logger.debug('Processing message', {
        messageId: message.id,
        partition: message.partition,
        offset: message.offset
      });

      // Store the message in MongoDB
      const result = await this.mongodbService.insertMessage(message);
      
      // Perform any additional business logic
      await this.performBusinessLogic(message);

      this.processedCount++;
      
      logger.debug('Message processed successfully', {
        messageId: message.id,
        insertedId: result.insertedId,
        skipped: result.skipped
      });

      return result;
    } catch (error) {
      this.errorCount++;
      logger.error('Failed to process message', {
        messageId: message.id,
        error: error.message
      });
      throw error;
    }
  }

  async processUserEvent(message, expectedAction) {
    try {
      // Validate that this is a user event with the expected action
      if (!message.value || message.value.action !== expectedAction) {
        throw new Error(`Expected action '${expectedAction}' but got '${message.value?.action}'`);
      }

      // Validate the user event structure
      const validatedEvent = validator.validateUserEvent(message.value);

      logger.info('Processing user event', {
        messageId: message.id,
        userId: validatedEvent.userId,
        action: validatedEvent.action,
        timestamp: validatedEvent.timestamp
      });

      // Store the message
      const result = await this.mongodbService.insertMessage(message);

      // Perform action-specific processing
      await this.processActionSpecificLogic(validatedEvent, message);

      this.processedCount++;

      logger.info('User event processed successfully', {
        messageId: message.id,
        userId: validatedEvent.userId,
        action: validatedEvent.action,
        insertedId: result.insertedId
      });

      return result;
    } catch (error) {
      this.errorCount++;
      logger.error('Failed to process user event', {
        messageId: message.id,
        expectedAction,
        error: error.message
      });
      throw error;
    }
  }

  async performBusinessLogic(message) {
    try {
      // Example business logic - update user session info
      if (message.value && message.value.userId && message.value.action) {
        await this.updateUserSession(message.value);
      }

      // Example - trigger alerts for specific actions
      if (message.value && message.value.action === 'login') {
        await this.checkForSuspiciousActivity(message.value);
      }

      // Example - update analytics
      await this.updateAnalytics(message);

    } catch (error) {
      logger.warn('Business logic processing failed', {
        messageId: message.id,
        error: error.message
      });
      // Don't throw here - business logic failures shouldn't stop message storage
    }
  }

  async processActionSpecificLogic(userEvent, message) {
    try {
      switch (userEvent.action) {
        case 'login':
          await this.handleLoginEvent(userEvent, message);
          break;
        case 'logout':
          await this.handleLogoutEvent(userEvent, message);
          break;
        case 'purchase':
          await this.handlePurchaseEvent(userEvent, message);
          break;
        case 'view_product':
          await this.handleProductViewEvent(userEvent, message);
          break;
        case 'add_to_cart':
          await this.handleCartEvent(userEvent, message, 'add');
          break;
        case 'remove_from_cart':
          await this.handleCartEvent(userEvent, message, 'remove');
          break;
        default:
          logger.debug('No specific handler for action', { 
            action: userEvent.action 
          });
      }
    } catch (error) {
      logger.warn('Action-specific processing failed', {
        messageId: message.id,
        action: userEvent.action,
        error: error.message
      });
    }
  }

  async handleLoginEvent(userEvent, message) {
    logger.debug('Processing login event', {
      userId: userEvent.userId,
      timestamp: userEvent.timestamp,
      ip: userEvent.metadata?.ip
    });

    // Example: Update last login time, check for multiple logins, etc.
    // This would typically involve additional database operations
  }

  async handleLogoutEvent(userEvent, message) {
    logger.debug('Processing logout event', {
      userId: userEvent.userId,
      timestamp: userEvent.timestamp,
      sessionId: userEvent.metadata?.sessionId
    });

    // Example: Calculate session duration, clean up session data, etc.
  }

  async handlePurchaseEvent(userEvent, message) {
    logger.debug('Processing purchase event', {
      userId: userEvent.userId,
      amount: userEvent.metadata?.amount,
      currency: userEvent.metadata?.currency,
      productId: userEvent.metadata?.productId
    });

    // Example: Update purchase history, trigger recommendations, etc.
  }

  async handleProductViewEvent(userEvent, message) {
    logger.debug('Processing product view event', {
      userId: userEvent.userId,
      productId: userEvent.metadata?.productId
    });

    // Example: Update view count, personalization data, etc.
  }

  async handleCartEvent(userEvent, message, action) {
    logger.debug(`Processing cart ${action} event`, {
      userId: userEvent.userId,
      productId: userEvent.metadata?.productId,
      action
    });

    // Example: Update cart state, trigger abandonment campaigns, etc.
  }

  async updateUserSession(userEvent) {
    try {
      // Example implementation - find and update user's latest session
      const query = {
        'value.userId': userEvent.userId,
        'value.sessionId': userEvent.metadata?.sessionId
      };

      const update = {
        lastActivity: new Date(userEvent.timestamp),
        lastAction: userEvent.action
      };

      // This is just an example - in practice you might use a separate collection
      // or service for session management
      logger.debug('Updating user session', {
        userId: userEvent.userId,
        sessionId: userEvent.metadata?.sessionId
      });
    } catch (error) {
      logger.warn('Failed to update user session', {
        userId: userEvent.userId,
        error: error.message
      });
    }
  }

  async checkForSuspiciousActivity(userEvent) {
    try {
      // Example: Check for rapid logins from different IPs
      const recentLogins = await this.mongodbService.findMessage({
        'value.userId': userEvent.userId,
        'value.action': 'login',
        timestamp: {
          $gte: new Date(Date.now() - 5 * 60 * 1000) // Last 5 minutes
        }
      });

      if (recentLogins.length > 3) {
        logger.warn('Suspicious login activity detected', {
          userId: userEvent.userId,
          recentLoginCount: recentLogins.length,
          currentIp: userEvent.metadata?.ip
        });

        // Could trigger alerts, temporary account locks, etc.
      }
    } catch (error) {
      logger.warn('Failed to check for suspicious activity', {
        userId: userEvent.userId,
        error: error.message
      });
    }
  }

  async updateAnalytics(message) {
    try {
      // Example: Increment counters, update aggregations, etc.
      logger.debug('Updating analytics', {
        messageId: message.id,
        action: message.value?.action
      });

      // This could involve updating separate analytics collections,
      // sending data to external analytics services, etc.
    } catch (error) {
      logger.warn('Failed to update analytics', {
        messageId: message.id,
        error: error.message
      });
    }
  }

  getMetrics() {
    return {
      processedCount: this.processedCount,
      errorCount: this.errorCount,
      successRate: this.processedCount > 0 ? 
        ((this.processedCount - this.errorCount) / this.processedCount * 100).toFixed(2) : 0
    };
  }

  resetMetrics() {
    this.processedCount = 0;
    this.errorCount = 0;
    logger.info('Message processor metrics reset');
  }
}

module.exports = MessageProcessor;