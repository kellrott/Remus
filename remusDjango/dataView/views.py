# Create your views here.


from django.http import HttpResponse
from django.conf import settings
from django.template import Context, loader

import json
import subprocess
import tempfile

import remus.db

def home(request):
    dbi = remus.db.connect(settings.REMUSDB_PATH)
    t = loader.get_template('instanceList.html')
    instList = []
    for inst in dbi.listInstances():
        row = { 'name' : inst }
        info = dbi.getInstanceInfo(inst)
        if "_description" in info:
            row['desc'] = info['_description']
        else:
            row['desc'] = ""
        instList.append(row)
        
    c = Context({
        'instanceList': instList,
    })
    return HttpResponse(t.render(c))


def instance(request, instance):
    dbi = remus.db.connect(settings.REMUSDB_PATH)
    t = loader.get_template('instancePage.html')
    tList = []
    showAll = False
    if 'showall' in request.GET:
        showAll = True
    for table in dbi.listTables(instance):
        if showAll or table.table.count('@') == 0:
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
        'tableName' : "/" + instance + "/" + table + ":" + key,
    	'attachList' : aList,
        'valueList': tList
    })
    return HttpResponse(t.render(c))

def attachment(request, instance, table, key, name):
    dbi = remus.db.connect(settings.REMUSDB_PATH)
    tmp = tempfile.NamedTemporaryFile(delete=True)
    tmp.close()
    tRef = remus.db.TableRef(instance, table)
    dbi.copyFrom(tmp.name, tRef, key, name)
    handle = open(tmp.name, "rb")
    return HttpResponse(handle, "text/plain")

    
def history(request, instance):
    dbi = remus.db.connect(settings.REMUSDB_PATH)
    wSet = {}
    text = "digraph G {"
    for table in dbi.listTables(instance):
        if table.table.endswith("@done"):
            for key, value in dbi.listKeyValue(table):
                tname = str(table.table).replace("@done", "") + key
                for inTable in value["input"]:
                    iname = inTable.split(':')[1]
                    hname = iname + "->" + tname
                    if hname not in wSet:
                        text += "\t\"%s\" -> \"%s\";\n" % (iname, tname)
                        wSet[hname] = True
                for outTable in value["output"]:
                    oname = outTable.split(':')[1]
                    hname = tname + "->" + oname
                    if hname not in wSet:
                        text += "\t\"%s\" -> \"%s\";\n" % (tname, oname)
                        wSet[hname] = True
    text += "}"
    p = subprocess.Popen( [ "dot", "-Tpng" ], stdin=subprocess.PIPE, stdout=subprocess.PIPE )
    stdoutdata, stderrdata = p.communicate(text)
    
    return HttpResponse(stdoutdata, "image/png" )
