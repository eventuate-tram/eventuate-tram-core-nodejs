const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const helpers = require('./lib/helpers');
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
        await kafkaProducer.send(topic, makeMessage(messageId, creationTime));
      } catch (err) {
        reject(err);
      }
    });
  });
});

function makeMessage(messageId, creationTime) {
   const headers = messageProducer.prepareMessageHeaders(topic, { headers: { ID: messageId, PARTITION_ID: 0, 'event-aggregate-type': eventAggregateType, 'event-type': eventType, DATE: creationTime }});
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