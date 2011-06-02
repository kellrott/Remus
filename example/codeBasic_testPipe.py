
@remus.pipe
def reduce( handles ):
	for key, value in handles[0]:
		remus.emit( "%s_%s" % (key, key), value * value )
