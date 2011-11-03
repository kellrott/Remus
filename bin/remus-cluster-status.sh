#!/bin/bash

for a in `cat host.list`; do 
	echo $a
	ssh $a ps auxww | grep remus;
done
