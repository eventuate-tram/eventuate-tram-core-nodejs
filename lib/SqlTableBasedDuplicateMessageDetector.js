const knex = require('./mysql/knex');
const { getLogger } = require('./utils');

const logger = getLogger({ title: 'SqlTableBasedDuplicateMessageDetector' });

const table = 'received_messages';
const DUPLICATE_ENTRY_CODE = 'ER_DUP_ENTRY';

class SqlTableBasedDuplicateMessageDetector {

  constructor(currentTimeInMillisecondsSql) {
    this.currentTimeInMillisecondsSql = typeof (currentTimeInMillisecondsSql) === 'string' ?
      new Date(currentTimeInMillisecondsSql).getTime() : currentTimeInMillisecondsSql;
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
      if (err.code === DUPLICATE_ENTRY_CODE) {
        return Promise.resolve(true);
      }

      return Promise.reject(err);
    }
  }

  // callback is application message handler
  async doWithMessage({ subscriberId, message }, callback) {
    try {
      const isDuplicate = await this.isDuplicate(subscriberId, message.messageId);
      if (!isDuplicate) {
        console.log('calling callback');
        return callback(message);
      }

      console.log('Message duplicated');
      return Promise.resolve();
    } catch (err) {
      logger.trace('Got exception - marking for rollback only', err);
      return Promise.reject(err);
    }
  }
}

module.exports = SqlTableBasedDuplicateMessageDetector;