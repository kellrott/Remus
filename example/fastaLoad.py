#!/usr/bin/env python

import sys
import Bio.SeqIO
from urllib import urlopen
import json

server = sys.argv[1]
fasta  = sys.argv[2]

handle = open( fasta )

for seq in Bio.SeqIO.parse( handle,"fasta"):
	print urlopen( server, json.dumps( {seq.id : seq.format("fasta") } ) ).read()
	
handle.close()
