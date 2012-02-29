
import tempfile
import os
def copy_instance(src_db, inst, dst_db):
    dst_db.createInstance(inst, src_db.getInstanceInfo(inst))
    for table in src_db.listTables(inst):
        dst_db.createTable(table, src_db.getTableInfo(table))
        for key, val in src_db.listKeyValue(table):
            dst_db.addData(table, key, val)
        for key in src_db.listKeys(table):
            for name in src_db.listAttachments(table,key):
                handle, tmp = tempfile.mkstemp()
                os.close(handle)
                src_db.copyFrom(tmp, table, key, name)
                dst_db.copyTo(tmp, table, key, name)
                os.unlink(tmp)
                
        
