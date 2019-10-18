class DomainEventPublisher {
  constructor({ messageProducer }) {
    this.messageProducer = messageProducer;
  }

  async publish(aggregateType, aggregateId, headers, domainEvents) {

    await Promise.all(domainEvents.map((event) => {
      this.messageProducer.send();
    }));

    /*for (domainEvents) {
      messageProducer.send(aggregateType,
                           makeMessageForDomainEvent(aggregateType, aggregateId, headers, event,
                             domainEventNameMapping.eventToExternalEventType(aggregateType, event)));

    }*/
  }
}

module.exports = DomainEventPublisher;