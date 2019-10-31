const OffsetTracker = require('./OffsetTracker');
const { getLogger, parseMessage } = require('../utils');

const logger = getLogger({ title: 'KafkaMessageProcessor' });

class KafkaMessageProcessor {
  constructor({ subscriberId, handler }) {

    this.subscriberId = subscriberId;
    this.handler = handler;
    this.offsetTracker = new OffsetTracker();
    this.processedRecords = [];
    this.notProcessedRecordsByMessageId = new Map();
  }

  async process(record) {
    const { topic, partition, offset } = record;
    this.offsetTracker.noteUnprocessed({ topic, partition }, offset);
    logger.debug('record: ', record);

    const { error, message } = parseMessage(record.value);

    if (error) {
      throw new Error(error);
    }

    this.notProcessedRecordsByMessageId.set(message.messageId, record);
    
    await this.handler(message);
    const processedRecord = this.notProcessedRecordsByMessageId.get(message.messageId);
    logger.debug(`Adding processed record to queue ${this.subscriberId} ${processedRecord.offset}`);

    this.processedRecords.push(processedRecord);
    return record
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