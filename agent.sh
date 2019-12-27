#!/usr/bin/env bash

file=$1

re="^([A-Z]|[a-z]){3}\s([A-Z]|[a-z]){3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}\s([0-9]{4})$"

lnum=0
lastLogNum=0

mkdir -p logs

start="0"

logFileId=0
currLogFile=

while read line; do
    ((lnum++))

    if [[ "$line" =~ ${re} ]]; then
        start="1"
    fi

    if [[ "$line" =~ ${re} && "${start}" == "1" ]]; then
        ((logFileId++))
        currLogFile="logs/$logFileId"
        lastLogNum=${lnum}
        let lastLogNum=${lnum}-1
    fi

    [[ ${start} == "1" ]] && echo "$line" >> $currLogFile
done < $file

echo "line $lnum"
echo "last log line $logFileId $lastLogNum"

cp file.txt file_copy.txt
sed -i "1,${lastLogNum}d" file_copy.txt
rm -f $currLogFile
