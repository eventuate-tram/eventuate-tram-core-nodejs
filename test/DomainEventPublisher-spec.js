const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const helpers = require('./lib/helpers');
const { KafkaConsumerGroup, DefaultChannelMapping, MessageProducer, DomainEventPublisher } = require('../');
const knex = require('../lib/mysql/knex');

chai.use(chaiAsPromised);

const channelMapping = new DefaultChannelMapping(new Map());
const messageProducer = new MessageProducer({ channelMapping });
const domainEventPublisher = new DomainEventPublisher({ messageProducer });
const kafkaConsumerGroup = new KafkaConsumerGroup();

const aggregateType = 'Account';
const aggregateId = 'Fake_aggregate_id';
const events = [
  { amount: 10, _type: 'CreditApproved' },
  { amount: 20, _type: 'CreditApproved' },
  { amount: 30, _type: 'CreditApproved' },
  { amount: 40, _type: 'CreditApproved' },
];
const groupId = 'test-domain-event-publisher-kcg-id';
const timeout = 20000;

let extraHeaders = {};

after(async () => {
  await kafkaConsumerGroup.unsubscribe();
});

describe('DomainEventPublisher', function () {
  this.timeout(timeout);

  it('makeMessageForDomainEvent() should return a correct message', () => {
    const messageForDomainEvent = domainEventPublisher.makeMessageForDomainEvent(aggregateType, aggregateId, extraHeaders, events[0], events[0]._type);
    console.log('messageForDomainEvent:', messageForDomainEvent);
    helpers.expectMessageForDomainEvent(messageForDomainEvent, events[0]);
  });

  it('should publish a message', async () => {
    return new Promise(async (resolve) => {
      kafkaConsumerGroup.on('message', (message) => {
        console.log('on message', message);
        helpers.expectKafkaMessage(message);
        resolve();
      });

      await kafkaConsumerGroup.subscribe({ groupId, topics: [ aggregateType ] });
      const trx = await knex.transaction();
      await domainEventPublisher.publish(aggregateType, aggregateId, events, { extraHeaders, trx });
      await trx.commit();
    });
  });
});
