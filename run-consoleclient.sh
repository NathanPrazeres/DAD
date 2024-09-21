#!/bin/bash
#THIS MUST RUN AFTER USING "docker-compose up"
docker exec -it dad bash -c "cd consoleclient && mvn exec:java"