
@remus.map
def homeFilter( key, value ):
	if value[ 'home' ].startswith( "/home" ):
		remus.emit( key, value )
