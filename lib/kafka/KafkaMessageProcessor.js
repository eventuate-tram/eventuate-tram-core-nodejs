const OffsetTracker = require('./OffsetTracker');
const { getLogger, makeEvent } = require('../utils');

const logger = getLogger({ title: 'KafkaMessageProcessor' });


class KafkaMessageProcessor {
  constructor({ subscriberId, handler }) {

    this.subscriberId = subscriberId;
    this.handler = handler;
    this.offsetTracker = new OffsetTracker();
    this.processedRecords = [];
    this.notProcessedRecordsByEventId = new Map();
  }

  async process(record) {
    const { topic, partition, offset } = record;
    this.offsetTracker.noteUnprocessed({ topic, partition }, offset);
    logger.debug('record: ', record);

    const { error, event } = makeEvent(record.value);

    if (error) {
      throw new Error(error);
    }

    this.notProcessedRecordsByEventId.set(event.eventId, record);
    
    const processedEvent = await this.handler(event);

    if (!processedEvent) {
      throw new Error('Event handler should return the event')
    }

    const record = this.notProcessedRecordsByEventId.get(processedEvent.eventId);

    logger.debug(`Adding processed record to queue ${this.subscriberId} ${record.offset}`);

    this.processedRecords.push(record);

    return record;
  }

  offsetsToCommit() {

    let count = 0;

    while (true) {

      const record = this.processedRecords.shift();

      if (!record) {
        break;
      }

      count++;

      const { topic, partition } = record;
      this.offsetTracker.noteProcessed({ topic, partition }, record.offset);
    }

    logger.debug(`Removed ${this.subscriberId} ${count} processed records from queue`);

    return this.offsetTracker.offsetsToCommit();
  }

  noteOffsetsCommitted(offsetsToCommit) {
    this.offsetTracker.noteOffsetsCommitted(offsetsToCommit);
  }

  getPending() {
    return this.offsetTracker;
  }
}

module.exports = KafkaMessageProcessor;