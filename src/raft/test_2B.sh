#!/bin/bash

test_names=("TestBasicAgree2B" "TestRPCBytes2B" "TestFollowerFailure2B" "TestLeaderFailure2B" "TestFailAgree2B" "TestFailNoAgree2B" "TestConcurrentStarts2B" "TestRejoin2B" "TestBackup2B" "TestCount2B")
#  test_names=("TestBackup2B" "TestCount2B")
# test_names=("TestPersist12C" "TestPersist22C" "TestPersist32C" "TestFigure82C" "TestUnreliableAgree2C" "TestFigure8Unreliable2C" "TestReliableChurn2C" "TestUnreliableChurn2C")
#test_names=("TestFigure8Unreliable2C" "TestReliableChurn2C" "TestUnreliableChurn2C")

j=0
total=${#test_names[*]}
while [ $j -lt $total ]; do
	echo "Running ${test_names[$j]}..."
	i=1
	flag=0
	while [ $i -le 50 ]; do
		date
		echo -e "\tRunning ${test_names[$j]} iteration $i..."
		rm -rf log*.txt
		echo ${test_names[$j]}
		output=$(go test -race -run ${test_names[$j]})
		echo "$output"
		
		if echo "$output" | grep -q "FAIL"; then
			echo "Test failed. Stopping the loop."
			flag=1
			curl -X POST "https://api.telegram.org/bot5403804301:AAH285DUy_5VHy2YpC846xmIODdWN5fQPB8/sendMessage" -d "chat_id=1003929699&text=${test_names[$j]} fail:iteration$i,$output"
			break
		fi
		
		i=$((i+1))
	done
	if (( $flag ==  1 )); then
		break
	fi
	curl -X POST "https://api.telegram.org/bot5403804301:AAH285DUy_5VHy2YpC846xmIODdWN5fQPB8/sendMessage" -d "chat_id=1003929699&text=${test_names[$j]} success"
	j=$((j+1))
done
