#!/usr/bin/env bash

mkdir -p .failed >/dev/null 2>&1
rm -rf .failed/* >/dev/null 2>&1
for i in {1..100}; do
	echo "Test $i start"
	# timeout 300 go test -race -run 2C > tmp.txt
	go test -race -run 2C > tmp.txt
	# EXIT_STATUS=$?
	if (grep -q FAIL tmp.txt ) ;then
		echo "$i failed;redirect to .failed/${i}.txt"
		 
		# locate the failed part
		python -c "import re;f=open(\"tmp.txt\",\"r\");print(re.sub(r\"Test[\s\S]*?Passed\",\"\",f.read(),flags=re.DOTALL));f.close()" > tmp2.txt
		# remove color 
		cat tmp2.txt| sed -e 's/\x1b\[36m//g' | sed -e 's/\x1b\[34m//g' | sed -e 's/\x1b\[31m//g'  | sed -e 's/\x1b\[0m//g'  > ".failed/${i}.txt"
		rm tmp.txt tmp2.txt  >/dev/null 2>&1
	else
		echo "$i succ";
		cat tmp.txt | grep "6.824/raft"|awk '{print $3}' >> .failed/result.txt
		rm tmp.txt tmp2.txt  >/dev/null 2>&1
	fi
done



