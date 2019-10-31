const { getLogger } = require('../utils');

const  logger = getLogger({ title: 'TopicPartitionOffsets' });

/**
 * Tracks the offsets for a TopicPartition that are being processed or have been processed
 */
class TopicPartitionOffsets {

  constructor() {
    /**
     * offsets that are being processed
     */
    this.unprocessed = [];

    /**
     * offsets that have been processed
     */

    this.processed = [];
  }

  toString() {
    return `unprocessed=[${this.unprocessed.join(', ')}], processed=[${this.processed.join(', ')}]`;
  }

  noteUnprocessed(offset) {
    this.unprocessed.push(offset);
  }

  noteProcessed(offset) {
    this.processed.push(offset);
  }

  /**
   * @return large of all offsets that have been processed and can be committed
   */
  offsetToCommit() {
    let result;
    for (let x of this.unprocessed) {
      if (this.processed.indexOf(x) < 0) {
        break;
      }
      result = x;
    }

    return result;
  }

  noteOffsetCommitted(offset) {
    this.unprocessed = this.unprocessed.filter(x => x >= offset);
    this.processed = this.processed.filter(x => x >= offset);
  }

  getPending() {
    return this.unprocessed.filter(x => this.processed.indexOf(x) < 0);
  }
}

module.exports = TopicPartitionOffsets;