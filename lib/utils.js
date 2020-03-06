const log4js = require('log4js');
const getLogger = ({ logLevel, title } = {}) => {
  const logger = log4js.getLogger(title || 'Logger');
  if (!logLevel) {
    logLevel = (process.env.NODE_ENV !== 'production') ? 'DEBUG' :' ERROR';
  }

  logger.level = logLevel;
  return logger;
};

const parseMessage = messageStr => {
  try {
    const { payload, headers } = JSON.parse(messageStr);
    const { ID: messageId, DESTINATION: topic, DATE: creationTime, PARTITION_ID: partitionId, ...rest } = headers;

    return {
      message: {
        messageId,
        topic,
        creationTime,
        partitionId,
        payload: JSON.parse(payload),
        ...rest,
      }
    };
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
  return new Promise(resolve => {
    setTimeout(resolve, timeout);
  });
};

module.exports = {
  getLogger,
  parseMessage,
  addZeroes,
  promisedDelay
};