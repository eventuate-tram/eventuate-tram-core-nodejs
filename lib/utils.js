const log4js = require('log4js');

const getLogger = ({ logLevel, title } = {}) => {

  const logger = log4js.getLogger(title || 'Logger');

  if (!logLevel) {
    logLevel = (process.env.NODE_ENV !== 'production') ? 'DEBUG' :' ERROR';
  }

  logger.setLevel(logLevel);

  return logger;
};

module.exports = {
  getLogger
};