
@remus.map
def map( key, val ):
	for i in range(int(val)):
		remus.emit( key, i )
