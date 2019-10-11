const util = require('util');
const { ConsumerGroup, Offset, KafkaClient } = require('kafka-node');
const isPortReachable = require('is-port-reachable');
const { getLogger } = require('../utils');
const KafkaMessageProcessor = require('./KafkaMessageProcessor');
const MessageConsumer = require('./MessageConsumer');

const logger = getLogger({ title: 'KafkaAggregateSubscriptions' });

class KafkaAggregateSubscriptions {
  constructor({ connectionString = '', fromOffset = 'latest' } = {}) {
    this.connectionString = connectionString || process.env.EVENTUATE_TRAM_ZOOKEEPER_CONNECTION_STRING;
    this.fromOffset = fromOffset;

    this.consumerGroupDefaultOptions = {
      host: this.connectionString,
      sessionTimeout: 30000,
      autoCommit: false,
      fromOffset,
    };

    this.connectionTimeout = 5000;
    this.messageConsumer = new MessageConsumer({ connectionString: this.connectionString, fromOffset });
  }

  async subscribe({ subscriberId, topics, messageHandler }) {
    const reachable = await this.checkKafkaIsReachable();
    logger.debug('reachable:', reachable);

    if (!reachable) {
      throw new Error(`Host unreachable ${this.connectionString}`);
    }

    await this.ensureTopicExistsBeforeSubscribing({ topics });

    await this.messageConsumer.subscribe({ groupId: subscriberId, topics });

    const processor = new KafkaMessageProcessor({ subscriberId, handler: messageHandler });

    this.messageConsumer.on('message', async (record) => {
      logger.debug('on message:', util.inspect(record, false, 10));
      logger.debug(`Processing record ${subscriberId} ${record.offset} ${record.value}`);

      try {
        let processedRecord = await processor.process(record);
        logger.debug(`Record processed {${subscriberId}}`, processedRecord);
        this.maybeCommitOffsets(this.messageConsumer.consumerGroup, processor);
      } catch (err) {
        logger.error(err);
        reject(err);
      }
    });
  }

  async unsubscribe() {
    logger.debug('unsubscribe()');
    await this.messageConsumer.unsubscribe();
  }

  disconnect() {
    return this.unsubscribe();
  }

  ensureTopicExistsBeforeSubscribing({ topics }) {
    const client = new KafkaClient(this.connectionString);

    return new Promise((resolve, reject) => {
      client.on('ready', () => {
        logger.debug('ensureTopicExistsBeforeSubscribing() client on ready');

        client.loadMetadataForTopics(topics, (err, resp) => {
          if (err) {
            return reject(err);
          }
          logger.debug('ensureTopicExistsBeforeSubscribing() loadMetadataForTopics resp:', JSON.stringify(resp));

          const [ , { metadata: respMetadata } ] = resp;

          Object.keys(respMetadata).forEach((topic) => {
            if (respMetadata.hasOwnProperty(topic)) {
              const partitions = Object.keys(respMetadata[topic])
                .map(index => respMetadata[topic][index])
                .map(metadata => metadata.partition);

              logger.debug(`Got these partitions for the Topic "${topic}": ${partitions.join(', ')}`);
            }
          });

          client.close((err) => {
            if (err) {
              return reject(err);
            }
            logger.debug('ensureTopicExistsBeforeSubscribing(): Client closed');
          });
          resolve(resp);
        });
      });

      client.on('error', reject);
    });
  }

  checkKafkaIsReachable() {
    const [ host, port ] = this.connectionString.split(':');
    const timeout = this.connectionTimeout;

    logger.debug(`checkKafkaIsReachable(): ${host}:${port}`);

    return isPortReachable(port, { host, timeout });
  }

  maybeCommitOffsets(consumer, processor) {

    const offsetsToCommit = processor.offsetsToCommit();
    logger.debug('KafkaAggregateSubscriptions::offsetsToCommit:', util.inspect(offsetsToCommit, false, 10));

    if (offsetsToCommit.length !== 0) {
      logger.debug(`Committing offsets ${consumer.options.groupId}`, util.inspect(offsetsToCommit, false, 10));

      consumer.commit(offsetsToCommit, () => {
        logger.debug(`Committed offsets ${consumer.options.groupId}`);
        logger.debug('offsetsToCommit:',util.inspect(offsetsToCommit, false, 10));
        processor.noteOffsetsCommitted(offsetsToCommit);
      });
    }
  }
}

module.exports = KafkaAggregateSubscriptions;
