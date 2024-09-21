#!/bin/bash
#THIS MUST RUN AFTER USING "docker-compose up"
docker exec -it dad bash -c "cd client && mvn exec:java"