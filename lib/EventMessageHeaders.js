class EventMessageHeaders {
  static get EVENT_TYPE() {
    return 'event-type';
  }
  static get AGGREGATE_TYPE() {
    return 'event-aggregate-type';
  }
  static get AGGREGATE_ID() {
    return 'event-aggregate-id';
  }
}

module.exports = EventMessageHeaders;
