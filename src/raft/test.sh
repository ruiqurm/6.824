#!/usr/bin/env bash

mkdir -p failed >/dev/null 2>&1
rm -rf failed/* >/dev/null 2>&1
for i in {1..10}; do
	echo "Test $i start"
	timeout 300 go test -race -run 2C > tmp.txt
	EXIT_STATUS=$?
	if (grep -q FAIL tmp.txt ) ;then
		echo "$i failed;redirect to failed/${i}.txt"
		python -c "import re;f=open(\"tmp.txt\",\"r\");print(re.sub(r\"Test[\s\S]*?Passed\",\"\",f.read(),flags=re.DOTALL));f.close()" > tmp2.txt
	   	mv tmp2.txt "failed/${i}.txt"
		rm tmp.txt
	else
		echo "$i succ";
	fi
done



