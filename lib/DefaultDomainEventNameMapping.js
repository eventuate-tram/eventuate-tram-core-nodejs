class DefaultDomainEventNameMapping {
  constructor() {}

  eventToExternalEventType(aggregateType, event) {
    event['event-type'] = aggregateType;
    return event;
  }

  externalEventTypeToEventClassName(aggregateType, eventTypeHeader) {
    return eventTypeHeader;
  }

}