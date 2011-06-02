
@remus.map
def map( key, val ):
	for i in range(int(val)):
		remus.emit( val, i, "val" )
	for i in range(len(key)):
		remus.emit( key, i, "key" )
	remus.emit( key, val )
