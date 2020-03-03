const MessageBuilder = require('./MessageBuilder');
const Message = require('./Message');
const { AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE } = require('./eventMessageHeaders');

class DomainEventPublisher {
  constructor({ messageProducer }) {
    this.messageProducer = messageProducer;
    this.messageBuilder = new MessageBuilder();
  }

  publish(aggregateType, aggregateId, domainEvents, { extraHeaders = {}, trx } = {}) {
    return domainEvents.reduce((p, event) => {
      const message = this.makeMessageForDomainEvent(aggregateType, aggregateId, extraHeaders, event, event._type);
      return p.then(_ => this.messageProducer.send(aggregateType, message, trx));
    }, Promise.resolve());
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
