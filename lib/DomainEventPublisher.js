const MessageBuilder = require('./MessageBuilder');
const Message = require('./Message');
const { AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE } = require('./eventMessageHeaders');


class DomainEventPublisher {
  constructor({ messageProducer }) {
    this.messageProducer = messageProducer;
    this.messageBuilder = new MessageBuilder();
  }

  async publish(aggregateType, aggregateId, headers, domainEvents) {
    await Promise.all(domainEvents.map((event) => {
      const message = this.makeMessageForDomainEvent(aggregateId, headers, event, event.eventType);
      this.messageProducer.send(aggregateType, message);
    }));
  }

  makeMessageForDomainEvent(aggregateType, aggregateId, headers, event, eventType) {
      const aggregateIdAsString = aggregateId.toString();
      return this.messageBuilder
        .withPayload(JSON.stringify(event))
        .withExtraHeaders('', headers)
        .withHeader(Message.PARTITION_ID, aggregateIdAsString)
        .withHeader(AGGREGATE_ID, aggregateIdAsString)
        .withHeader(AGGREGATE_TYPE, aggregateType)
        .withHeader(EVENT_TYPE, eventType)
        .build();
  }
}

module.exports = DomainEventPublisher;
