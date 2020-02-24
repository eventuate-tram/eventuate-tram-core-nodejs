const util = require('util');
const { KafkaClient } = require('kafka-node');
const isPortReachable = require('is-port-reachable');
const { ensureEnvVariables } = require('../env');
const { getLogger } = require('../utils');
const KafkaMessageProcessor = require('./KafkaMessageProcessor');
const KafkaConsumerGroup = require('./KafkaConsumerGroup');
const messageHandlerDecorator = require('../messageHandlerDecorator');

const logger = getLogger({ title: 'MessageConsumer' });

const [ kafkaBootstrapServers ] = ensureEnvVariables([ 'EVENTUATE_TRAM_KAFKA_BOOTSTRAP_SERVERS' ]);

class MessageConsumer {
  constructor({ fromOffset = 'latest' } = {}) {
    this.connectionString = kafkaBootstrapServers;

    this.fromOffset = fromOffset;

    this.connectionTimeout = 5000;
    this.kafkaConsumerGroup = new KafkaConsumerGroup({ fromOffset: this.fromOffset });
  }

  async subscribe({ subscriberId, topics, messageHandler }) {
    const decoratedHandler = messageHandlerDecorator(messageHandler, subscriberId);
    const reachable = await this.checkKafkaIsReachable();
    logger.debug('reachable:', reachable);

    if (!reachable) {
      throw new Error(`Host unreachable ${this.connectionString}`);
    }

    await this.ensureTopicExistsBeforeSubscribing({ topics });

    await this.kafkaConsumerGroup.subscribe({ groupId: subscriberId, topics });

    const processor = new KafkaMessageProcessor({ handler: decoratedHandler });

    this.kafkaConsumerGroup.on('message', async (record) => {
      logger.debug('On message ${subscriberId}:', util.inspect(record, false, 10));

      try {
        await processor.process(record);
        this.maybeCommitOffsets(this.kafkaConsumerGroup.consumerGroup, processor);
      } catch (err) {
        logger.error(err);
        throw err;
      }
    });
  }

  async unsubscribe() {
    logger.debug('unsubscribe()');
    await this.kafkaConsumerGroup.unsubscribe();
  }

  disconnect() {
    return this.unsubscribe();
  }

  ensureTopicExistsBeforeSubscribing({ topics }) {
    const client = new KafkaClient({ kafkaHost: this.connectionString });

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

    const offsetsToCommit = processor.offsetTracker.offsetsToCommit();

    logger.debug('offsetsToCommit:', util.inspect(offsetsToCommit, false, 10));

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

module.exports = MessageConsumer;
