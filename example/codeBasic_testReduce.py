
@remus.reduce
def reduce( key, vals ):
	remus.emit( key, len(list(vals)) )
