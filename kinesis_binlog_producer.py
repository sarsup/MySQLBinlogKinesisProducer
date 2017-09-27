
# coding: utf-8

# In[ ]:

import json
import boto3
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
  DeleteRowsEvent,
  UpdateRowsEvent,
  WriteRowsEvent,
)
from datetime import datetime, date
from decimal import Decimal
from pymysql.err import InternalError
import os
import sys
import traceback
try:
    dir_path = os.path.dirname(os.path.realpath(__file__))
    os.chdir(dir_path)
except Exception as e:
    print ("Assuming code run from notebook as " + str(e))
PROJECTNAME = 'data-warehouse'
cwd = os.getcwd().split('/')
ind = cwd.index(PROJECTNAME)
projectroot = "/".join(cwd[:ind+1])
sys.path.insert(0,projectroot)
from conf import dbconfig as cfg
from conf import connections as conn


# In[ ]:

def getBinlogStream(connection_dict=cfg.bitspring_slave,server_id=145106,log_file=None,log_pos=None):
    
    stream = BinLogStreamReader(
        connection_settings= connection_dict,
        server_id=server_id,
        blocking=True,
        resume_stream=True,
        log_file = log_file,
        log_pos = log_pos,
        only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent])
    return stream


# In[ ]:

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        serial = obj.isoformat()
        return serial
    if isinstance(obj,Decimal):
        return float(obj)
    raise TypeError ("Type %s not serializable" % type(obj))


# In[ ]:

def kinesisBinlogProducer(stream,kinesisClient,log_file=None,log_pos=None):

    loop = True
    log_file = log_file
    log_pos=log_pos
    restartnum = 0
    while loop:
        try:
            for binlogevent in stream:
                restartnum=0
                if log_file!=stream.log_file:
                    print log_file, stream.log_file
                    log_file = stream.log_file
                log_pos = binlogevent.packet.log_pos
                structure = {}
                for col in binlogevent.table_map[binlogevent.table_id].column_schemas:
                    structure[col['COLUMN_NAME']] = col['COLUMN_TYPE']
                for row in binlogevent.rows:
                    event = {"schema": binlogevent.schema,
                    "table": binlogevent.table,
                    "type": type(binlogevent).__name__,
                    "metadata_timestamp": binlogevent.timestamp,
                    "primary_key" : binlogevent.__dict__['primary_key'],
                    "structure" : structure,
                    "row": row
                    }
                    kinesisClient.put_record(StreamName="YOUR_STREAM_NAME", 
                                       Data=json.dumps(event, default = json_serial), 
                                       PartitionKey="default")

        except InternalError as e:
            '''get stream again with previous log_file and log_pos'''
            exc_type, exc_value, exc_traceback = sys.exc_info()
            print repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
            print "Restarting stream with " + stream.log_file + " " + str(log_pos)
            stream = getBinlogStream(log_file=log_file,log_pos=log_pos)
            restartnum +=1
            if restartnum>=10:
                raise InternalError(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))

        except KeyboardInterrupt as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            print "User requested stop"
            loop = False

        finally:
            with open("last_binlog_position","w") as sbinlog:
                sbinlog.write(log_file+","+str(log_pos))


# In[ ]:

def main():
    sess = boto3.Session(profile_name="bigdata")
    kinesis = sess.client("kinesis")
    log_pos=None
    log_file=None
    stream = getBinlogStream(log_file=log_file,log_pos=log_pos)
    kinesisBinlogProducer(stream,kinesis,log_file,log_pos)
  


# In[ ]:

if __name__ == "__main__":
    main()

