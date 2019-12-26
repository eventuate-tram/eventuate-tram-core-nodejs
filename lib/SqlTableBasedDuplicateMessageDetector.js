const knex = require('./mysql/knex');
const { getLogger } = require('./utils');

const logger = getLogger({ title: 'SqlTableBasedDuplicateMessageDetector', logLevel: 'debug' });

const table = 'received_messages';
const duplicateKeyRegex = /^duplicate key/;

class SqlTableBasedDuplicateMessageDetector {

  constructor(currentTimeInMillisecondsSql, transactionTemplate) {
    this.currentTimeInMillisecondsSql = currentTimeInMillisecondsSql;
    this.transactionTemplate = transactionTemplate;
  }

  async isDuplicate(consumerId, messageId) {
    const receivedMessage = {
      consumer_id: consumerId,
      message_id: messageId,
      creation_time: this.currentTimeInMillisecondsSql
    };

    try {
      await knex(table).insert(receivedMessage);
      return false;
    } catch (err) {
      return duplicateKeyRegex.test(err.message);
    }
  }

  async doWithMessage({ subscriberId, messageId }, callback) {
    await this.transactionTemplate();
    try {
      const isDuplicate = await this.isDuplicate(subscriberId, messageId);
      if (!isDuplicate) {
        return callback();
      }

      return null;
    } catch (err) {
      logger.trace("Got exception - marking for rollback only", e);
      this.transactionTemplate.rollback();
      throw err;
    }
  }
}

module.exports = SqlTableBasedDuplicateMessageDetector;