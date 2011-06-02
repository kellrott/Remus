
@remus.merge
def merge( lkey, lvals, rkey, rvals ):
	word = None
	for val in lvals:
		word = val
	count = None
	for val in rvals:
		count = val
	remus.emit( "%s_%s" % (lkey,rkey), [word] * count )
