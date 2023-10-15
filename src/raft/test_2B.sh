#!/bin/bash

test_names=("TestBasicAgree2B" "TestRPCBytes2B" "TestFollowerFailure2B" "TestLeaderFailure2B" "TestFailAgree2B" "TestFailNoAgree2B" "TestConcurrentStarts2B" "TestRejoin2B" "TestBackup2B" "TestCount2B")

j=0
total=${#test_names[*]}
while [ $j -lt $total ]; do
	echo "Running ${test_names[$j]}..."
	i=1
	while [ $i -le 100 ]; do
		echo -e "\tRunning ${test_names[$j]} iteration $i..."
		rm -rf log*.txt
		echo ${test_names[$j]}
		output=$(go test -run ${test_names[$j]})
		echo "$output"
		
		if echo -e "\t$output" | grep -q "FAIL"; then
			echo "Test failed. Stopping the loop."
			break
		fi
		
		i=$((i+1))
	done
	j=$((j+1))
done
