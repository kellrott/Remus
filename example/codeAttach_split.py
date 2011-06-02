
@remus.split
def split( info ):
	i = 0
	varList = [ 'var1', 'var2', 'var3', 'var4' ]
	for var in varList:
		handle = remus.open( var, "test.file", "w" )
		for j in range( info['lineCount'] ):
			handle.write( "%s test line %d = %s\n" % ( var, i, info['infoText'] ) )
			i += 1
		handle.close()
		remus.emit( var, "test.file" )
