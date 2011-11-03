#!/bin/bash


for a in `cat host.list `; do
	ssh $a pkill -f remus_db.yaml
done

pkill -f remus_master.yaml

