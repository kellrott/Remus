# Create your views here.


from django.http import HttpResponse
from django.conf import settings
from django.template import Context, loader

import json
import remus.db

def home(request):
    dbi = remus.db.connect(settings.REMUSDB_PATH)
    t = loader.get_template('instanceList.html')
    c = Context({
        'instanceList': dbi.listInstances(),
    })
    return HttpResponse(t.render(c))


def instance(request, instance):
    dbi = remus.db.connect(settings.REMUSDB_PATH)
    t = loader.get_template('instanceList.html')
    tList = []
    for table in dbi.listTables(instance):
        tList.append("/" + table.instance + table.table)
    c = Context({
        'instanceList': tList,
    })
    return HttpResponse(t.render(c))

def table(request, instance, table):
    dbi = remus.db.connect(settings.REMUSDB_PATH)
    t = loader.get_template('tableList.html')
    tList = []
    tRef = remus.db.TableRef(instance, table)
    for key in dbi.listKeys(tRef):
        tList.append(key)
    c = Context({
        'tableName' : "/" + instance + "/" + table,
        'instanceList': tList,
    })
    return HttpResponse(t.render(c))
    
def key(request, instance, table, key):
    dbi = remus.db.connect(settings.REMUSDB_PATH)
    t = loader.get_template('valueList.html')
    tList = []
    tRef = remus.db.TableRef(instance, table)
    for val in dbi.getValue(tRef, key):
        tList.append(json.dumps(val))
    aList = []
    for name in dbi.listAttachments(tRef, key):
    	aList.append(name)
    c = Context({
    	'attachList' : aList,
        'valueList': tList
    })
    return HttpResponse(t.render(c))
    
