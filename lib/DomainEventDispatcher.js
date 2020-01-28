const eventMessageHeaders = require('./eventMessageHeaders');
const { getLogger } = require('./utils');

const logger = getLogger({ title: 'DomainEventDispatcher' });

class DomainEventDispatcher {

  constructor(eventDispatcherId, domainEventHandlers, messageConsumer, domainEventNameMapping) {
    this.eventDispatcherId = eventDispatcherId;
    this.domainEventHandlers = domainEventHandlers;
    this.messageConsumer = messageConsumer;
    this.domainEventNameMapping = domainEventNameMapping;
  }

  initialize() {
    return this.messageConsumer.subscribe({
      subscriberId: this.eventDispatcherId,
      topics: Object.keys(this.domainEventHandlers),
      messageHandler: this.messageHandler
    });
  }


  messageHandler(message) {
    const aggregateType = message.getRequiredHeader(eventMessageHeaders.AGGREGATE_TYPE);
    const eventType =  message.getRequiredHeader(eventMessageHeaders.EVENT_TYPE);
    const mappedHeader = this.domainEventNameMapping.externalEventTypeToEventClassName(aggregateType, eventType);
    message.setHeader(eventMessageHeaders.EVENT_TYPE, mappedHeader);

    const handler = this.domainEventHandlers[aggregateType][eventType];

    if (!handler) {
      return Promise.resolve();
    }

    return handler(message);
  }
}

module.exports = DomainEventDispatcher;