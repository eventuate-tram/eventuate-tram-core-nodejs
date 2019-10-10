require('./ensureMysqlEnv');
const mysql = require('promise-mysql');
const { getLogger } = require('../utils');
const config = require('./config');

const logger = getLogger({ title: 'MySql' });

logger.debug('Connection options', Object.assign(config, { connectionLimit: 10 }));

const pool = mysql.createPool(config);

const getConnection = () => pool;

module.exports = {
  getConnection
};
