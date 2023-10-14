#!/bin/bash

i=1
while [ $i -le 100 ]; do
    echo "Running test iteration $i..."
    rm -rf log*.txt
    output=$(go test -run 2A)
    echo $output
    
    if [[ $output != *"Passed"* ]]; then
        echo "Test failed. Stopping the loop."
        break
    fi
    
    i=$((i+1))
done
