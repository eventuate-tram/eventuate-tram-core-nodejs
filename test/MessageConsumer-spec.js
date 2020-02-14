const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const helpers = require('./lib/helpers');
const { AGGREGATE_TYPE: AGGREGATE_TYPE_HEADER, EVENT_TYPE: EVENT_TYPE_HEADER, AGGREGATE_ID: AGGREGATE_ID_HEADER } = require('../lib/eventMessageHeaders');
const { MessageProducer, KafkaProducer, IdGenerator, MessageConsumer } = require('../');

chai.use(chaiAsPromised);

const messageConsumer = new MessageConsumer();
const kafkaProducer = new KafkaProducer();
const idGenerator = new IdGenerator();
const messageProducer = new MessageProducer();

const timeout = 20000;
const topic = 'test-topic';
const eventAggregateType = 'Account';
const eventType = 'charge';

before(async () => {
  await kafkaProducer.connect();
});

after(async () => {
  await Promise.all([
    messageConsumer.disconnect(),
    kafkaProducer.disconnect()
  ]);
});

describe('MessageConsumer', function () {
  this.timeout(timeout);

  it('should ensureTopicExistsBeforeSubscribing()', async () => {
    const result = await messageConsumer.ensureTopicExistsBeforeSubscribing({ topics: [ topic ]});
    helpers.expectEnsureTopicExists(result);
  });

  it('should receive Kafka message', async () => {
    const subscriberId = 'test-message-consumer-sb-id';
    return new Promise(async (resolve, reject) => {
      const messageHandler = (message) => {
        console.log('messageHandler');
        console.log(message);
        // TODO: expect message
        resolve();
        return Promise.resolve();
      };

      try {
        await messageConsumer.subscribe({ subscriberId, topics: [ topic ], messageHandler });

        const messageId = await idGenerator.genIdInternal();
        const creationTime = new Date().toUTCString();
        const message = await makeMessage(messageId, creationTime);
        await kafkaProducer.send(topic, message);
      } catch (err) {
        reject(err);
      }
    });
  });
});

async function makeMessage(messageId, creationTime) {
  const headers = await messageProducer.prepareMessageHeaders(topic, {
   headers: {
     ID: messageId,
     PARTITION_ID: 0,
     [AGGREGATE_TYPE_HEADER]: eventAggregateType,
     [EVENT_TYPE_HEADER]: eventType,
     DATE: creationTime
   }}
  );
  return JSON.stringify({
    payload: JSON.stringify({ message: 'Test kafka subscription' }),
    headers,
    offset: 5,
    partition: 0,
    highWaterOffset: 6,
    key: '0',
    timestamp: creationTime
  });
}