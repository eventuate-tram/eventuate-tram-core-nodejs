#!/usr/bin/env bash

docker-compose -f docker-compose-mysql-binlog-kafka.yml up -d

npm test

docker-compose -f docker-compose-mysql-binlog-kafka.yml down -v