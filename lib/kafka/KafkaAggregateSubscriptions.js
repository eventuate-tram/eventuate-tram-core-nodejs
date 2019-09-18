const { ConsumerGroup, Offset, KafkaClient } = require('kafka-node');
const isPortReachable = require('is-port-reachable');
const { getLogger } = require('../utils');
const KafkaMessageProcessor = require('./KafkaMessageProcessor');
const util = require('util');


const logger = getLogger({ title: 'KafkaAggregateSubscriptions' });

class KafkaAggregateSubscriptions {

  constructor({ connectionString = '', fromOffset = 'earliest' } = {}) {
    this.connectionString = connectionString || process.env.EVENTUATELOCAL_ZOOKEEPER_CONNECTION_STRING;
    this.fromOffset = fromOffset;

    this.consumerGroupDefaultOptions = {
      host: this.connectionString,
      sessionTimeout: 30000,
      autoCommit: false,
      fromOffset,
    };

    this.connectionTimeout = 5000;
  }

  async subscribe({ subscriberId, topics, eventHandler }) {

    const reachable = await this.checkKafkaIsReachable();

    logger.debug('reachable:', reachable);

    if (!reachable) {
      throw new Error(`Host unreachable ${this.connectionString}`);
    }

    await this.ensureTopicExistsBeforeSubscribing({ topics });

    return this.createConsumerGroup({ groupId: subscriberId, topics, eventHandler });
  }

  ensureTopicExistsBeforeSubscribing({ topics }) {

    const client = new KafkaClient(this.connectionString);

    return new Promise((resolve, reject) => {

      client.on('ready', () => {

        logger.debug('client on ready');

        client.loadMetadataForTopics(topics, (err, resp) => {

          if (err) {
            return reject(err);
          }

          logger.debug('loadMetadataForTopics resp:', JSON.stringify(resp));

          for(let topic in resp[1].metadata) {

            if (resp[1].metadata.hasOwnProperty(topic)) {
              const partitions = Object.keys(resp[1].metadata[topic])
                .map(index => resp[1].metadata[topic][index])
                .map(metadata => {
                  return metadata.partition;
                });

              logger.debug(`Got these partitions for Topic ${topic}: ${partitions.join(', ')}`);
            }
          }

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

    return isPortReachable(port, { host, timeout })
  }

  createConsumerGroup({ groupId, topics, eventHandler }) {

    logger.debug('{ groupId, topics, eventHandler }', { groupId, topics, eventHandler });

    return new Promise((resolve, reject) => {

      const options = Object.assign(this.consumerGroupDefaultOptions, { groupId });

      logger.debug('options:', options);

      const consumerGroup = new ConsumerGroup(options, topics);

      const processor = new KafkaMessageProcessor({ subscriberId: groupId, handler: eventHandler });

      consumerGroup.on('message', async (record) => {

        logger.debug('on message:', util.inspect(record, false, 10));

        logger.debug(`Processing record ${groupId} ${record.offset} ${record.value}`);


        try {

          const processedRecord = await processor.process(record);

          logger.debug(`Record processed {${groupId}}`, processedRecord);

          this.maybeCommitOffsets(consumerGroup, processor);
        } catch (err) {

          logger.error(err);
          reject(err);
        }
      });

      consumerGroup.on('connect', () => {
        logger.debug(`Consumer Group '${groupId}' connected.`);
        resolve(consumerGroup);
      });

      consumerGroup.on('error', (err) => {
        logger.error('ON error: ', err);
        // TODO: better handle { code: 'ECONNRESET' }
        reject(err);
      });

      consumerGroup.client.on('error', (data) => {
        logger.error('client ON error: ', data);
      });
    });
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