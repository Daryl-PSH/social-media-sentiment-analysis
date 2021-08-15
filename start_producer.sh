#!/usr/bin/env bash

PRODUCER_SCRIPT_PATH=src/data_extractor/tweet_producer.py

echo 'Giving Kafka a bit of time to start up…'
sleep 30

python $PRODUCER_SCRIPT_PATH