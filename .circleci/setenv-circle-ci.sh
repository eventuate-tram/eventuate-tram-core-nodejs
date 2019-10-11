
# Host DNS name doesnt resolve in Docker alpine images

export DOCKER_HOST_IP=$(hostname -I | sed -e 's/ .*//g')
export TERM=dumb

export EVENTUATE_TRAM_MYSQL_HOST=$DOCKER_HOST_IP
export EVENTUATE_TRAM_MYSQL_PORT=3306
export EVENTUATE_TRAM_MYSQL_DATABASE=eventuate
export EVENTUATE_TRAM_MYSQL_USERNAME=mysqluser
export EVENTUATE_TRAM_MYSQL_PASSWORD=mysqlpw
