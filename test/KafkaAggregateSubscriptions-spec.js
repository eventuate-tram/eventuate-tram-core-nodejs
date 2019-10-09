const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const { ConsumerGroup } = require('kafka-node');
const helpers = require('./lib/helpers');
const KafkaAggregateSubscriptions = require('../lib/kafka/KafkaAggregateSubscriptions');
const IdGenerator = require('../lib/IdGenerator');
const KafkaProducer = require('../lib/kafka/KafkaProducer');
const MessageProducer = require('../lib/MessageProducer');

chai.use(chaiAsPromised);

const kafkaAggregateSubscriptions = new KafkaAggregateSubscriptions();
const kafkaProducer = new KafkaProducer();
const idGenerator = new IdGenerator();
const messageProducer = new MessageProducer();

const timeout = 20000;
const topic = 'test-topic';
const eventAggregateType = 'Account';
const eventType = 'charge';

after(async () => {
  await kafkaAggregateSubscriptions.disconnect();
  await kafkaProducer.disconnect();
});

describe('KafkaAggregateSubscriptions', function () {
  this.timeout(timeout);

  it('should ensureTopicExistsBeforeSubscribing()', async () => {
    const result = await kafkaAggregateSubscriptions.ensureTopicExistsBeforeSubscribing({ topics: [ topic ]});
    helpers.expectEnsureTopicExists(result);
  });

  it('should receive Kafka message', async () => {
    await kafkaProducer.connect();

    const subscriberId = 'test-sb-id';
    return new Promise(async (resolve, reject) => {
      const messageHandler = (message) => {
        console.log('messageHandler');
        console.log(message);
        resolve();
        return Promise.resolve(message);
      };

      try {
        const subscription = await kafkaAggregateSubscriptions.subscribe({ subscriberId, topics: [ topic ], messageHandler });
        expect(subscription).to.be.instanceOf(ConsumerGroup);

        const messageId = await idGenerator.genIdInternal();
        const creationTime = new Date().getTime();
        const headers = messageProducer.prepareMessageHeaders(topic, { id: messageId, partitionId: 0, eventAggregateType, eventType });
        const message = JSON.stringify({
            payload: JSON.stringify({ message: 'Test kafka subscription' }),
            headers,
            offset: 5,
            partition: 0,
            highWaterOffset: 6,
            key: '0',
            timestamp: new Date(creationTime).toUTCString()
          });
        kafkaProducer.send(topic, message);
      } catch (err) {
        reject(err);
      }
    });
  });
});