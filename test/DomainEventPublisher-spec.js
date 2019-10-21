const chai = require('chai');
const { expect } = chai;
const chaiAsPromised = require('chai-as-promised');
const helpers = require('./lib/helpers');
const DomainEventPublisher = require('../lib/DomainEventPublisher');
const IdGenerator = require('../lib/IdGenerator');
const MessageProducer = require('../lib/MessageProducer');

chai.use(chaiAsPromised);


const messageProducer = new MessageProducer();
const idGenerator = new IdGenerator();
const domainEventPublisher = new DomainEventPublisher({ messageProducer });

const aggregateType = 'Account';
const aggregateId = 'Fake_aggregate_id';
const eventType = 'charge';
const event = { amount: 100 };
const topic = 'test-domain-event-publisher';
const creationTime = new Date().getTime();

let headers;

before(async () => {
  const messageId = await idGenerator.genIdInternal();
  headers = messageProducer.prepareMessageHeaders(topic, {
    id: messageId, partitionId: 0, creationTime, eventAggregateType: aggregateType, eventType
  });
});

describe('DomainEventPublisher', () => {
  it('makeMessageForDomainEvent() should return a correct message', () => {
    const messageForDomainEvent = domainEventPublisher.makeMessageForDomainEvent(aggregateType, aggregateId, headers, event, eventType);
    console.log('messageForDomainEvent:', messageForDomainEvent);
    helpers.expectMessageForDomainEvent(messageForDomainEvent, event);
  });

  it('should publish a message', async () => {
    await domainEventPublisher.publish(aggregateType, aggregateId, headers, [ event ]);
  });
});
