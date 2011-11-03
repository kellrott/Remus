#!/bin/bash

for a in `cat host.list`; do 
	ssh $a ~/RemusCluster/remusDB.sh; 
done

~/RemusCluster/remusMaster.sh

