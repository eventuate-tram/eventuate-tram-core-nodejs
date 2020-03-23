const { EVENT_TYPE } = require('./eventMessageHeaders');

class DefaultDomainEventNameMapping {
  constructor(mappings) {
    this.mappings = mappings;
    // {
    //  [aggregateType]: eventMappings;// new Map([iterable])
    // }
  }

  eventToExternalEventType(aggregateType, event) {
    event[EVENT_TYPE] = aggregateType;
    return event;
  }

  externalEventTypeToEvent(aggregateType, eventTypeHeader) {
    if (this.mappings[aggregateType]) {
      return this.mappings[aggregateType].get(eventTypeHeader);
    }
    return eventTypeHeader;
  }
}

module.exports = DefaultDomainEventNameMapping;