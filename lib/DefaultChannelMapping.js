const { getLogger } = require('./utils');

const logger = getLogger({ title: 'DefaultChannelMapping' });

class DefaultChannelMapping {

  constructor(mappings) {
    this.mappings = mappings;// new Map([iterable])
  }

  with(fromChannel, toChannel) {
    this.mappings.set(fromChannel, toChannel);
  }

  transform(channel) {
    const mappingChannel = this.mappings.get(channel);
    return mappingChannel || channel;
  }
}

module.exports = DefaultChannelMapping;