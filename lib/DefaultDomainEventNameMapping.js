const { EVENT_TYPE } = require('./eventMessageHeaders');

class DefaultDomainEventNameMapping {
  constructor() {}

  eventToExternalEventType(aggregateType, event) {
    event[EVENT_TYPE] = aggregateType;
    return event;
  }

  externalEventTypeToEventClassName(aggregateType, eventTypeHeader) {
    return eventTypeHeader;
  }
}

module.exports = DefaultDomainEventNameMapping;