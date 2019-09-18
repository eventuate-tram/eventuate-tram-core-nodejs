const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const { ConsumerGroup } = require('kafka-node');
const KafkaAggregateSubscriptions = require('../lib/kafka/KafkaAggregateSubscriptions');

chai.use(chaiAsPromised);

const connectionString = process.env.EVENTUATELOCAL_ZOOKEEPER_CONNECTION_STRING;
const kafkaAggregateSubscriptions = new KafkaAggregateSubscriptions({ connectionString });
const timeout = 30000;


describe('KafkaAggregateSubscriptions', function () {
  this.timeout(timeout);

  it('should connect to Kafka', async () => {
    const subscriberId = 'test-sb-id';
    const topic = 'test';
    const eventHandler = (event) => {
      console.log('eventHandler');
      console.log(event);
    };

    const subscription = await kafkaAggregateSubscriptions.subscribe({ subscriberId, topics: [ topic ], eventHandler });
    expect(subscription).to.be.instanceOf(ConsumerGroup);
  });
});