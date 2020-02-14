const MessageBuilder = require('./MessageBuilder');
const Message = require('./Message');
const { AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, EVENT_DATA } = require('./eventMessageHeaders');

class DomainEventPublisher {
  constructor({ messageProducer }) {
    this.messageProducer = messageProducer;
    this.messageBuilder = new MessageBuilder();
  }

  async publish(aggregateType, aggregateId, extraHeaders, domainEvents, trx) {
    await Promise.all(domainEvents.map((event) => {
      const message = this.makeMessageForDomainEvent(aggregateType, aggregateId, extraHeaders, event[EVENT_DATA], event[EVENT_TYPE]);
      return this.messageProducer.send(aggregateType, message, trx);
    }));
  }

  makeMessageForDomainEvent(aggregateType, aggregateId, extraHeaders, event, eventType) {
      const aggregateIdAsString = aggregateId.toString();
      return this.messageBuilder
        .withPayload(JSON.stringify(event))
        .withExtraHeaders('', extraHeaders)
        .withHeader(Message.PARTITION_ID, aggregateIdAsString)
        .withHeader(AGGREGATE_ID, aggregateIdAsString)
        .withHeader(AGGREGATE_TYPE, aggregateType)
        .withHeader(EVENT_TYPE, eventType)
        .build();
  }
}

module.exports = DomainEventPublisher;
