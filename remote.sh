#!/bin/bash

rsync --exclude build --delete --delete-excluded -r . "$1:~/ycsb-squared"
ssh "$1" "sudo systemctl stop journalwatch.timer"
ssh -L localhost:16686:localhost:16686 -L localhost:9090:localhost:9090 "$1" -C "cd ~/ycsb-squared && \
  podman-compose -f docker-compose.local.yml up --build --force-recreate"
