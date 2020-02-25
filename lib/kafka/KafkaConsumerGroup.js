const util = require('util');
const { EventEmitter } = require('events');
const { getLogger } = require('../utils');
const { ConsumerGroup } = require('kafka-node');
const { ensureEnvVariable } = require('../env');

const logger = getLogger({ title: 'KafkaConsumerGroup' });

const connectionString = ensureEnvVariable('EVENTUATE_TRAM_KAFKA_BOOTSTRAP_SERVERS');

class KafkaConsumerGroup extends EventEmitter {

  constructor({ fromOffset = 'latest' } = {}) {
    super();

    this.connectionString = connectionString;
    this.fromOffset = fromOffset;

    this.consumerGroupDefaultOptions = {
      host: this.connectionString,
      sessionTimeout: 30000,
      autoCommit: false,
      fromOffset,
    };

    this.connectionTimeout = 5000;
    this.consumerGroup = null;
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
        this.emit('message', message);
      });

      this.consumerGroup.on('error', (err) => {
        logger.error('ON error: ', err);
        reject(err);
      });

      this.consumerGroup.client.on('error', (err) => {
        logger.error('client ON error: ', err);
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

module.exports = KafkaConsumerGroup;