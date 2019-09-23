const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const { ConsumerGroup } = require('kafka-node');
const helpers = require('./lib/helpers');
const KafkaAggregateSubscriptions = require('../lib/kafka/KafkaAggregateSubscriptions');

chai.use(chaiAsPromised);

const kafkaAggregateSubscriptions = new KafkaAggregateSubscriptions();
const timeout = 50000;
const topic = 'test-topic';

describe('KafkaAggregateSubscriptions', function () {
  this.timeout(timeout);

  xit('should ensureTopicExistsBeforeSubscribing()', async () => {
    const result = await kafkaAggregateSubscriptions.ensureTopicExistsBeforeSubscribing({ topics: [ topic ]});
    console.log('result:' , result);
    helpers.expectEnsureTopicExists(result);
  });

  it('should receive Kafka message', async () => {
    const subscriberId = 'test-sb-id';
    return new Promise(async (resolve, reject) => {
      const eventHandler = (event) => {
        console.log('eventHandler');
        console.log(event);
        resolve();
        return Promise.resolve(event);
      };

      try {
        const subscription = await kafkaAggregateSubscriptions.subscribe({ subscriberId, topics: [ topic ], eventHandler });
        expect(subscription).to.be.instanceOf(ConsumerGroup);
        await helpers.putMessage(topic, JSON.stringify({ eventData: { message: 'Test message' } }));
      } catch (err) {
        reject(err);
      }
    });
  });
});