
import datetime
@remus.agent
def subwatch( key, value ):
	dateStr = datetime.date.today().isoformat()	
	subKey = "test-" + dateStr
	value[ '_input' ] = { "_instance" : "dataRoot", "_applet" : "dataStack" }
	value[ '_applets' ] = [ "homeFilter" ] 
	remus.emit( subKey, value )
