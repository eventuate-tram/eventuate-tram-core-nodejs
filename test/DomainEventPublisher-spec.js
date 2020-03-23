const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const helpers = require('./lib/helpers');
const { KafkaConsumerGroup, MessageProducer, DomainEventPublisher } = require('../');
const knex = require('../lib/mysql/knex');

chai.use(chaiAsPromised);

const aggregateType = 'Account';
const aggregateId = 'Fake_aggregate_id';
const events = [
  { amount: 10, _type: 'CreditApproved' },
  { amount: 20, _type: 'CreditApproved' },
  { amount: 30, _type: 'CreditApproved' },
  { amount: 40, _type: 'CreditApproved' },
];
const timeout = 20000;

describe('DomainEventPublisher', function () {
  this.timeout(timeout);

  it('makeMessageForDomainEvent() should return a correct message', () => {
    const domainEventPublisher = new DomainEventPublisher({ messageProducer: new MessageProducer() });
    const messageForDomainEvent = domainEventPublisher.makeMessageForDomainEvent(aggregateType, aggregateId, {}, events[0], events[0]._type);
    helpers.expectMessageForDomainEvent(messageForDomainEvent, events[0]);
  });

  it('should publish a message', async () => {
    return new Promise(async (resolve) => {

      const kafkaConsumerGroup = new KafkaConsumerGroup();
      const groupId = 'test-domain-event-publisher-kcg-id';

      kafkaConsumerGroup.on('message', async (message) => {
        helpers.expectKafkaMessage(message);
        await kafkaConsumerGroup.unsubscribe();
        resolve();
      });

      await kafkaConsumerGroup.subscribe({ groupId, topics: [ aggregateType ] });

      const domainEventPublisher = new DomainEventPublisher({ messageProducer: new MessageProducer() });

      const trx = await knex.transaction();
      await domainEventPublisher.publish(aggregateType, aggregateId, events, { trx });
      await trx.commit();
    });
  });

  it('should publish a message with provided channel mapping', async () => {
    return new Promise(async (resolve) => {

      const kafkaConsumerGroup = new KafkaConsumerGroup();
      const groupId = 'test-domain-event-publisher-channel-mapping-kcg-id';

      kafkaConsumerGroup.on('message', async (message) => {
        helpers.expectKafkaMessage(message);
        await kafkaConsumerGroup.unsubscribe();
        resolve();
      });

      await kafkaConsumerGroup.subscribe({ groupId, topics: [ aggregateType ] });

      class CustomChannelMapping {
        constructor() {
          // map "CustomerAccount" aggregate type to "Account"
          this.mappings = new Map([['CustomerAccount', 'Account']]);
        }
        transform(channel) {
          const mappingChannel = this.mappings.get(channel);
          return mappingChannel || channel;
        }
      }

      const messageProducer = new MessageProducer({ channelMapping: new CustomChannelMapping() });
      const domainEventPublisher = new DomainEventPublisher({ messageProducer });

      const trx = await knex.transaction();
      await domainEventPublisher.publish('CustomerAccount', aggregateId, events, { trx });
      await trx.commit();
    });
  });
});
