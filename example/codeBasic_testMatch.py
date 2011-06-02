
@remus.match
def reduce( key, lvals, rvals ):
	sum = 0
	for valList in lvals:
		for val in valList:
			sum += int(val)
	square = None
	for val in rvals:
		square = val
	if sum == square:
		remus.emit( key, "GOOD" )
	else:
		remus.emit( key, "BAD" )
