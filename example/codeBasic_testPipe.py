
@remus.pipe
def pipe( handles ):
	for key, value in handles["testReduce"]:
		remus.emit( "%s_%s" % (key, key), value * value )
