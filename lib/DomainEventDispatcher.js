const { AGGREGATE_TYPE, EVENT_TYPE } = require('./eventMessageHeaders');
const { getLogger } = require('./utils');
const DefaultDomainEventNameMapping = require('./DefaultDomainEventNameMapping.js');

const logger = getLogger({ title: 'DomainEventDispatcher' });

class DomainEventDispatcher {

  constructor({ eventDispatcherId, domainEventHandlers, messageConsumer, domainEventNameMapping }) {
    this.eventDispatcherId = eventDispatcherId;
    this.domainEventHandlers = domainEventHandlers;
    this.messageConsumer = messageConsumer;
    this.domainEventNameMapping = domainEventNameMapping || new DefaultDomainEventNameMapping({});
  }

  initialize() {
    return this.messageConsumer.subscribe({
      subscriberId: this.eventDispatcherId,
      topics: Object.keys(this.domainEventHandlers),
      messageHandler: this.messageHandler.bind(this)
    });
  }

  messageHandler(message) {
    const aggregateType = message[AGGREGATE_TYPE];
    const eventType =  message[EVENT_TYPE];
    message[EVENT_TYPE] = this.domainEventNameMapping.externalEventTypeToEvent(aggregateType, eventType);

    const externalEventType =  message[EVENT_TYPE];
    const handler = this.domainEventHandlers[aggregateType][externalEventType];

    if (!handler) {
      return Promise.resolve();
    }

    return handler(message);
  }
}

module.exports = DomainEventDispatcher;