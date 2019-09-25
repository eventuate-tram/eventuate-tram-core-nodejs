const invariant = require('invariant');

[
  'EVENTUATE_TRAM_MYSQL_HOST',
  'EVENTUATE_TRAM_MYSQL_PORT',
  'EVENTUATE_TRAM_MYSQL_DATABASE',
  'EVENTUATE_TRAM_MYSQL_USERNAME',
  'EVENTUATE_TRAM_MYSQL_PASSWORD'
].forEach(env => invariant(process.env[env], `set value for ${env} environment variable`));
