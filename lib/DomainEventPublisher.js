const MessageBuilder = require('./MessageBuilder');
const Message = require('./Message');
const EventMessageHeaders = require('./EventMessageHeaders');

class DomainEventPublisher {
  constructor({ messageProducer }) {
    this.messageProducer = messageProducer;
  }

  async publish(aggregateType, aggregateId, headers, domainEvents) {

    await Promise.all(domainEvents.map((event) => {
      this.messageProducer.send(aggregateType, DomainEventPublisher.makeMessageForDomainEvent(aggregateId, headers, event, event.eventType));
    }));
  }

  static makeMessageForDomainEvent(aggregateType, aggregateId, headers, event, eventType) {
      const aggregateIdAsString = aggregateId.toString();
      return MessageBuilder
        .withPayload(JSON.stringify(event))
        .withExtraHeaders('', headers)
        .withHeader(Message.PARTITION_ID, aggregateIdAsString)
        .withHeader(EventMessageHeaders.AGGREGATE_ID, aggregateIdAsString)
        .withHeader(EventMessageHeaders.AGGREGATE_TYPE, aggregateType)
        .withHeader(EventMessageHeaders.EVENT_TYPE, eventType)
        .build();
  }
}

module.exports = DomainEventPublisher;
