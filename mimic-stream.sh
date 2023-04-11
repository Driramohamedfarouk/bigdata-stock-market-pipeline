#!/bin/bash

dir="./sp500/csv"



for file in $dir/*; do
        first_line=$(sed -n '2p' $file)
        echo "$file,$first_line" | kafka-console-producer.sh --broker-list localhost:9092 --topic stock-market
        sleep 1
done
