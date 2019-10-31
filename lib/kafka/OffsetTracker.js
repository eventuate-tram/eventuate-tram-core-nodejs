const TopicPartitionOffsets = require('./TopicPartitionOffsets');
const { getLogger } = require('../utils');

const  logger = getLogger({ title: 'OffsetTracker' });

class OffsetTracker {
  constructor() {
    this.state = {};
  }

  fetch(topicPartition) {
    let tpo = this.getState(topicPartition);

    if (!tpo) {
      tpo = new TopicPartitionOffsets();
      this.putState(topicPartition, tpo);
    }

    return tpo;
  }

  getState({ topic, partition }) {
    if (this.state[topic] && this.state[topic][partition]) {
      return this.state[topic][partition]
    }
  }

  putState({ topic, partition }, tpo) {
    if (!this.state[topic]) {
      this.state[topic] = {};
    }

    if (!this.state[topic][partition]) {
      this.state[topic][partition] = {};
    }

    this.state[topic][partition] = tpo;
  }

  noteUnprocessed(topicPartition, offset) {
    const tpo = this.fetch(topicPartition);
    tpo.noteUnprocessed(offset);
  }

  noteProcessed(topicPartition, offset) {
    const tpo = this.fetch(topicPartition);
    tpo.noteProcessed(offset);
  }

  noteOffsetsCommitted(offsetsToCommit) {
    Object.keys(offsetsToCommit).forEach(topic => {
      const partitions = offsetsToCommit[topic];

      Object.keys(partitions).forEach(partition => {
        const offsets = partitions[partition];

        offsets.forEach(({ offset }) => {
          const tpo = this.fetch({ topic, partition });
          tpo.noteOffsetCommitted(offset);
        });
      });
    });
  }

  offsetsToCommit() {
    const result = {};

    Object.keys(this.state).forEach(topic => {
      const partitions = this.state[topic];

      Object.keys(partitions).forEach(partition => {
        const tpo = partitions[partition];
        const offsetToCommit = tpo.offsetToCommit();

        if (!offsetToCommit) {
          return logger.debug('No offsetToCommit');
        }

        if (!result[topic]) {
          result[topic] = {};
        }

        if (!result[topic][partition]) {
          result[topic][partition] = [];
        }

        result[topic][partition].push({offset: offsetToCommit + 1});
      });
    });

    return result;
  }
}

module.exports = OffsetTracker;
