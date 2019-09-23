const mysql = require('promise-mysql');
const invariant = require('invariant');
const { getLogger } = require('../utils');

const logger = getLogger({ title: 'MySql' });

[
  'EVENTUATE_TRAM_MYSQL_HOST',
  'EVENTUATE_TRAM_MYSQL_PORT',
  'EVENTUATE_TRAM_MYSQL_DATABASE',
  'EVENTUATE_TRAM_MYSQL_USERNAME',
  'EVENTUATE_TRAM_MYSQL_PASSWORD'
].forEach(env => invariant(process.env[env], `set value for ${env} environment variable`));

const options = {
  host: process.env.EVENTUATE_TRAM_MYSQL_HOST,
  port: process.env.EVENTUATE_TRAM_MYSQL_PORT,
  user: process.env.EVENTUATE_TRAM_MYSQL_USERNAME,
  password : process.env.EVENTUATE_TRAM_MYSQL_PASSWORD,
  connectionLimit: 10,
  database: process.env.EVENTUATE_TRAM_MYSQL_DATABASE,
};

logger.debug('Connection options', options);

const pool = mysql.createPool(options);

const getConnection = () => pool;

module.exports = {
  getConnection
};
