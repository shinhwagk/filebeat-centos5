#!/bin/bash

endline=$(python /workspaces/filebeat-centos5/lib.py)


echo $endline

sed -i "1,${endline}d" delete.file 