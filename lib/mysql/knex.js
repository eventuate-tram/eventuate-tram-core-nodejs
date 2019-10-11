const config = require('./config');
const knex = require('knex') ({
  client: 'mysql',
  connection: config,
  debug: true,
  pool: { min: 0, max: 10 }
});

module.exports = knex;