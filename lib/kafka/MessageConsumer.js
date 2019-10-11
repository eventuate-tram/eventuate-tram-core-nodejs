const util = require('util');
const { EventEmitter } = require('events');
const { getLogger } = require('../utils');
const { ConsumerGroup, Offset, KafkaClient } = require('kafka-node');

const logger = getLogger({ title: 'MessageConsumer' });

class MessageConsumer extends EventEmitter {

  constructor({ connectionString = '', fromOffset = 'latest' } = {}) {
    super();

    this.connectionString = connectionString || process.env.EVENTUATE_TRAM_ZOOKEEPER_CONNECTION_STRING;
    this.fromOffset = fromOffset;

    this.consumerGroupDefaultOptions = {
      host: this.connectionString,
      sessionTimeout: 30000,
      autoCommit: false,
      fromOffset,
    };

    this.connectionTimeout = 5000;
  }

  async subscribe({ groupId, topics }) {
    return new Promise((resolve, reject) => {
      const options = Object.assign(this.consumerGroupDefaultOptions, { groupId });
      logger.debug('options:', options);

      this.consumerGroup = new ConsumerGroup(options, topics);

      this.consumerGroup.on('connect', () => {
        logger.debug(`Consumer Group '${groupId}' connected.`);
        resolve(this.consumerGroup);
      });

      this.consumerGroup.on('message', (message) => {
        logger.debug('on message:', util.inspect(message, false, 10));
        this.emit('message', message);
      });

      this.consumerGroup.on('error', (err) => {
        logger.error('ON error: ', err);
        reject(err);
      });

      this.consumerGroup.client.on('error', (data) => {
        logger.error('client ON error: ', data);
        reject(err);
      });
    });
  }

  unsubscribe() {
    logger.debug('unsubscribe()');
    return new Promise((resolve, reject) => {
      this.consumerGroup.close((err, result) => {
        if (err) {
          logger.error('unsubscribe() error:', err);
          return reject(err);
        }
        logger.debug('unsubscribe() success');
        resolve(result);
      });
    });
  }
}

module.exports = MessageConsumer;