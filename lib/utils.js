const log4js = require('log4js');

const getLogger = ({ logLevel, title } = {}) => {
  const logger = log4js.getLogger(title || 'Logger');
  if (!logLevel) {
    logLevel = (process.env.NODE_ENV !== 'production') ? 'DEBUG' :' ERROR';
  }

  logger.level = logLevel;
  return logger;
};

const parseEvent = eventStr => {
  try {
    const { id: eventId, eventType, entityId, entityType, eventData: eventDataStr, swimlane, eventToken } = JSON.parse(eventStr);
    const eventData = JSON.parse(eventDataStr);
    const event = {
      eventId,
      eventType,
      entityId,
      swimlane,
      eventData,
      eventToken,
      entityType,
    };

    return { event };
  } catch (error) {
    return { error };
  }
};

const addZeroes = ({ src , width = 16, z = '0', position = 'begin' }) => {

  if (typeof src !== 'string') {
    src = src.toString();
  }

  if (src.length >= width) {
    return src;
  }

  const zeros = new Array(width - src.length + 1).join(z);

  if (position === 'end') {
    return src + zeros;
  }

  return zeros + src;
};

const promisedDelay = (timeout) => {
  console.log(`promisedDelay: ${timeout}`);
  return new Promise(resolve => {
    setTimeout(resolve, timeout);
  });
};

const makeMessageHeaders = ({ messageId, partitionId, topic, creationTime, eventAggregateType, eventType }) => JSON.stringify({
  ID: messageId,
  PARTITION_ID: partitionId,
  DESTINATION: topic,
  DATE: new Date(creationTime).toUTCString(),
  'event-aggregate-type': eventAggregateType,
  'event-type': eventType
});

module.exports = {
  getLogger,
  parseEvent,
  addZeroes,
  promisedDelay,
  makeMessageHeaders
};