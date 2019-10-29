const { ensureEnvVariables } = require('../env');

const [ host, port, user, password, database ] = ensureEnvVariables([
  'EVENTUATE_TRAM_MYSQL_HOST',
  'EVENTUATE_TRAM_MYSQL_PORT',
  'EVENTUATE_TRAM_MYSQL_USERNAME',
  'EVENTUATE_TRAM_MYSQL_PASSWORD',
  'EVENTUATE_TRAM_MYSQL_DATABASE'
]);

module.exports = { host, port, user, password, database };
