#!/usr/local/bin/python2.7

import cx_Oracle
from multiprocessing import Pool
import subprocess
import sys
import datetime
import fnmatch
import os
import logging
import json
import glob
import urllib2
import shutil
import re

fstart = datetime.datetime.now()
fstart_time = fstart.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
feed=str(sys.argv[1])
runid=str(sys.argv[2])

################################ Get values from parameter file ################################
prmfname = "/data/juniper/jar/Cextract/Cextractor_oracle_param.txt"
fo = open(prmfname, "rw+")
line = fo.read().split()
baseloc=line[0].split('=')[1]
extloc=line[1].split('=')[1]
logloc=line[2].split('=')[1]
lockey=line[3].split('=')[1]
metalogon=line[4].split('=')[1]
decjar=line[5].split('=')[1]
nifiurl=line[6].split('=')[1]
fo = open(metalogon, "rw+")
logonline = fo.read().split()
constrmeta=logonline[0]

############################### Get values from metadata 1 ######################################
connection = cx_Oracle.connect(constrmeta)
cur = connection.cursor()
now = datetime.datetime.now()
dte= now.strftime("%d%m%Y")
vdte = now.strftime("%Y%m%d")
dtm= now.strftime("%Y-%m-%d")
ldtm= now.strftime("%Y-%m-%d-%H-%M-%S")
log=logloc+feed+"_"+runid+"_"+ldtm+".log"
logging.basicConfig(filename=log,level=logging.DEBUG)
logging.info('Script commencing - %s',fstart)
tgtquery = """select a.TARGET_TYPE,b.country_code,b.FEED_SEQUENCE,b.PROJECT_SEQUENCE,d.ACCESS_TOKEN_URL,d.ACCESS_TOKEN_USER_NAME,d.ACCESS_TOKEN_PASSWORD,e.PARALLEL_INP
from 
JUNIPER_EXT_TARGET_CONN_MASTER a, 
JUNIPER_EXT_FEED_MASTER b, 
JUNIPER_EXT_FEED_SRC_TGT_LINK c,
NIFI_ACCESS_TOKEN_PROVIDER d,
JUNIPER_EXT_PARALLEL e
where 
a.TARGET_CONN_SEQUENCE = c.TARGET_SEQUENCE and 
b.feed_sequence = c.feed_sequence and
b.feed_unique_name = '%s'""" % (feed)
cur.execute(tgtquery)
tgtsql = cur.fetchall()
tgttyp= tgtsql[0][0]
dcc= tgtsql[0][1]
dfid=tgtsql[0][2]
dprjid=tgtsql[0][3]
tokenurl=tgtsql[0][4]
nifiusr=tgtsql[0][5]
nifipwd=tgtsql[0][6]
prll=tgtsql[0][7]
niuser=nifiusr+":"+nifipwd

################################ Conversion process ############################################
def Conversion_process(dtbl,tblcnt,where_clause,constr,dwhr):
  logging.info('Commencing Conversion process to avro')

  izip = zip
  xrange = range
  feed_name=str(sys.argv[1])
  file_path=extloc+dcc+"/"+feed_name+"/"+vdte+"/"+runid+"/"
  meta_path=extloc+dcc+"/"+feed_name+"/"+vdte+"/"+runid+"/metadata/"
  def grouper(n, sequence):
    for i in xrange(0, len(sequence), n):
        yield sequence[i:i+n]

  connection = cx_Oracle.connect(constr)
  cur = connection.cursor()
  tbl=dtbl.split('.')[1]
  col_sts = """select COLUMN_NAME,DATA_TYPE,COLUMN_ID,'string'
from all_tab_columns %s""" % (where_clause)
  cur.execute(col_sts)
  sqlcol = cur.fetchall()
  print sqlcol
  for i in sqlcol:
   colnm,coltyp,colpos = i[0],i[1],i[2]
   insertcmd="insert into JUNIPER_EXT_COLUMN_STATUS ( COUNTRY_CODE,FEED_ID,FEED_UNIQUE_NAME,RUN_ID,EXTRACTED_DATE,TABLE_NAME,COLUMN_NAME,COLUMN_TYPE,PROJECT_SEQUENCE,COLUMN_POS) values('%s',%s,'%s','%s','%s','%s','%s','%s',%s,%s)" % (dcc,dfid,feed,runid,vdte,dtbl,colnm,coltyp,dprjid,str(colpos))
   meta_file=meta_path+dtbl+".sql"
   file = open(meta_file,"a")
   file.write( insertcmd + "\n")
  file.close() 
  columns_where_clause=where_clause.replace("'","$")
################################ Fetching Column name for json #################################
  titles = ['name']
  Cvalues = [e[0] for g in grouper(1, sqlcol) for e in g]
  Ckeys = (titles[i%1] for i in xrange(len(Cvalues)))
  Cobjs = [dict(g) for g in grouper(1, list(izip(Ckeys, Cvalues)))]
################################ Fetching Column type for json #################################
  Dtitles = ['type']
  Dvalues = [e[3] for g in grouper(2, sqlcol) for e in g]
  Dkeys = (Dtitles[i%1] for i in xrange(len(Dvalues)))
  Dobjs = [dict(g) for g in grouper(1, list(izip(Dkeys, Dvalues)))]
################################ Fetching final json ###########################################
  col_typ=list(zip(Cobjs,Dobjs))
  init = {'type':'record','name':tbl,'fields':col_typ}
  conv=json.dumps(init).replace("[{","{")
  jsnt1=conv.replace("}, {",", ")
  jsnt2=jsnt1.replace("}]","}")
################################ NiFi datatype handler #########################################
#  jsntb=jsnt2.replace('"NUMBER"','["null","string"]').replace('"VARCHAR"','["null","string"]').replace('"VARCHAR2"','["null","string"]').replace('"TIMESTAMP(6)"','["null","string"]').replace('"DATE"','["null","string"]').replace('"CLOB"','["null","string"]').replace('"BLOB"','["null","record"]').replace('"CHAR"','["null","string"]')
  jsntb=jsnt2.replace('"string"','["null","string"]')
  if tblcnt == 0:
   data_availability = 'N'
  else:
   data_availability = 'Y' 
  file_name = feed_name+"_"+dtbl+"_"+runid+"*.csv"
  convin= {"process_group": 1,
                "date": vdte,
                "file_path": file_path,
                "run_id": runid,
                "file_name": file_name,
                "avro_conv_flg": "Y",
                "feed_id": dfid,
                "feed_name": feed_name,
                "country_code": dcc,
                "project_sequence": dprjid,
                "file_type": "Delimited",
                "file_sequence": 1,
                "table_name":dtbl,
                "field_list": jsntb,
                "file_delimiter": ",",
                "data_availability":data_availability,
                "table_count":tblcnt,
                "where_clause":dwhr
}
  conv2=json.dumps(convin)
  print conv2
  permfile=extloc+dcc
  subprocess.call(['chmod', '-R', '777', permfile])
  logging.info('Jsontonifi = %s',conv2)
############################### get nifi token and trigger nifi for avro conversion ############ 
  curtok1 = "curl -s %s -H 'Content-Type:application/x-www-form-urlencoded; charset=UTF-8' --data 'username=%s&password=%s' --compressed --insecure"%(tokenurl,nifiusr,nifipwd)
  curtok = subprocess.Popen(curtok1,shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT).stdout.read()
  precurnifi = "curl -i -u " + niuser + " -H '"
  postcurnifi = "' -H 'Content-Type: application/json' -X POST -d '" + str(conv2) + "' %s --compressed --insecure"%(nifiurl)
  nifiexfile = "%snifiexfile.txt" %(logloc)
  file = open(nifiexfile,"a")
  file.write( precurnifi )
  file.write( curtok + postcurnifi )
  file.close()
  with open(nifiexfile, "r") as myfile:
   data=myfile.read()
  print data
  os.remove(nifiexfile)
  curtrg = subprocess.Popen(data,shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT).stdout.read()
  logging.info('Nifi trigger status %s',curtrg)
  nifiexstatus = "%snifiexstatus.txt" %(logloc)
  file = open(nifiexstatus,"a")
  file.write( curtrg )
  file.close()
  if 'HTTP/1.1 200 OK' in open(nifiexstatus).read():
    logging.info('Nifi Trigger successfull')
  else:
    logging.debug('Nifi Trigger failed')
    connection = cx_Oracle.connect(constrmeta)
    cur = connection.cursor()
    nifisql = """UPDATE JUNIPER_EXT_NIFI_STATUS SET STATUS = 'FAILED', JOB_END_TIME=CURRENT_TIMESTAMP, COMMENTS = 'Unable to establish connection with Nifi server, please check logs - %s%s'  
WHERE PROJECT_SEQUENCE = %s and FEED_ID = %s and RUN_ID = '%s'""" %(logloc,log,dprjid,dfid,runid)
    print nifisql
    cur.execute(nifisql)
    nifistat = cur.fetchall()

  os.remove(nifiexstatus)
############################### Get values from metadata 2 ######################################
if tgttyp == 'HDFS':
  metaquery = """select b.host_name,b.port_no,b.service_name,b.username,b.password,b.encrypted_encr_key,
a.country_code,a.feed_sequence,c.TARGET_TYPE,a.project_sequence,f.src_conn_sequence,
c.TARGET_CONN_SEQUENCE,c.HDP_KNOX_HOST,c.HDP_USER,c.HDP_ENCRYPTED_PASSWORD,c.ENCRYPTED_KEY,c.HDP_HDFS_PATH
from
JUNIPER_EXT_FEED_MASTER a,
JUNIPER_EXT_SRC_CONN_MASTER b,
JUNIPER_EXT_TARGET_CONN_MASTER c,
JUNIPER_EXT_TABLE_MASTER e,
JUNIPER_EXT_FEED_SRC_TGT_LINK f
where
a.feed_sequence = f.feed_sequence and
f.src_conn_sequence = b.src_conn_sequence and
f.TARGET_SEQUENCE=c.TARGET_CONN_SEQUENCE and
a.feed_sequence = e.feed_sequence and
feed_unique_name='%s'""" % (feed)

else:
  metaquery = """select b.host_name,b.port_no,b.service_name,b.username,b.password,b.encrypted_encr_key,
a.country_code,a.feed_sequence,d.bucket_name,a.project_sequence,f.src_conn_sequence,
d.GCP_SEQUENCE,d.GCP_PROJECT,d.SERVICE_ACCOUNT_NAME,d.SERVICE_ACCOUNT_KEY,d.ENCRYPTED_ENCR_KEY,coalesce(null,'0',c.HDP_HDFS_PATH) as HDP_HDFS_PATH 
from 
JUNIPER_EXT_FEED_MASTER a,
JUNIPER_EXT_SRC_CONN_MASTER b, 
JUNIPER_EXT_TARGET_CONN_MASTER c,
JUNIPER_EXT_GCP_MASTER d, 
JUNIPER_EXT_TABLE_MASTER e, 
JUNIPER_EXT_FEED_SRC_TGT_LINK f
where 
a.feed_sequence = f.feed_sequence and
f.src_conn_sequence = b.src_conn_sequence and 
f.TARGET_SEQUENCE=c.TARGET_CONN_SEQUENCE and 
c.gcp_sequence = d.gcp_sequence and 
a.feed_sequence = e.feed_sequence and
feed_unique_name='%s'""" % (feed)

cur.execute(metaquery)
j = cur.fetchall()
################################ Oracle Data extraction - Common source/target #################
for i in j:
 dhost, dprt, dsrvc, dusr, dpwd, denyk,dcc,dfid,dbck,dprjid,dscs,dtcs,dhdphst,dhdpusr,dhdppwd,dhdpenyk,dhdppth = i[0],i[1],i[2],i[3],i[4],i[5],i[6],i[7],i[8],i[9],i[10],i[11],i[12],i[13],i[14],i[15],i[16]

sjr = 'java -cp %sdecryptionJar-0.0.1-SNAPSHOT.jar com.infy.gcp.decryption.Decrypt JUNIPER_EXT_SRC_CONN_MASTER SRC_CONN_SEQUENCE %s PASSWORD ENCRYPTED_ENCR_KEY %s %s' %(decjar,dscs,lockey,logloc)
subprocess.Popen(sjr,shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT).stdout.read()
subprocess.call(sjr,shell=True)
sfilename = logloc+"JUNIPER_EXT_SRC_CONN_MASTER_"+str(dscs)+"_PASSWORD.txt"
fo = open(sfilename, "rw+")
line = fo.read().split()
spassw= line[0]
os.remove(logloc+"JUNIPER_EXT_SRC_CONN_MASTER_"+str(dscs)+"_PASSWORD.txt")
host=dhost+":"+dprt+"/"+dsrvc
constr=dusr+"/"+spassw+"@"+host

djbnm=feed+"_read"
logging.info('Inserting into JUNIPER_EXT_NIFI_STATUS as running: %s',feed)
qnifisql = "Insert into JUNIPER_EXT_NIFI_STATUS (COUNTRY_CODE,FEED_ID,FEED_UNIQUE_NAME,RUN_ID,NIFI_PG,PG_TYPE,EXTRACTED_DATE,JOB_START_TIME,STATUS,PROJECT_SEQUENCE,JOB_NAME,JOB_TYPE) values ('%s',%s,'%s','%s',1,'AVROCONV','%s',to_timestamp('%s','DD-MON-RR HH.MI.SSXFF AM'),'running',%s,'%s','R')" %(dcc,dfid,feed,runid,vdte,fstart_time,dprjid,djbnm)
print qnifisql
logging.info('qnifisql: %s',qnifisql)
cur.execute(qnifisql)
connection.commit()

############################### Get values from metadata 3(Table level) ########################
Table_level = """select e.table_name,e.columns,e.where_clause,e.fetch_type,e.incr_col
from JUNIPER_EXT_TABLE_MASTER e
where e.feed_sequence='%s'""" % (dfid)

cur.execute(Table_level)
result = cur.fetchall()

for i in result:
  dtbl,dcol,dwhr,dtyp,dincl = i[0],i[1],i[2],i[3],i[4]
  start = datetime.datetime.now()
  start_time = start.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
  user=dtbl.split('.')[0]
  #passw="cdc1"
  TABLE=dtbl.split('.')[1]
  tstampdt='TIMESTAMP%' 
#Substitue all column names and cast for timestamp
  if dcol == 'all' or dcol == 'ALL':
   allcol = """select COLUMN_NAME from all_tab_columns where TABLE_NAME = '%s' and DATA_TYPE <> 'BLOB' and DATA_TYPE not like '%s'""" % (TABLE,tstampdt)
   tstampcol = """select COLUMN_NAME from all_tab_columns where TABLE_NAME = '%s' and DATA_TYPE like '%s'""" % (TABLE,tstampdt)
   blobcol = """select COLUMN_NAME from all_tab_columns where TABLE_NAME = '%s' and DATA_TYPE like 'BLOB'""" % (TABLE)
   print allcol
   print tstampcol
   print blobcol
   connection = cx_Oracle.connect(constr)
   cur = connection.cursor()
   cur.execute(allcol)
   resultallcol = cur.fetchall()
   joinallcol = ",".join(str(x) for x in resultallcol)
   replaceallcol=joinallcol.replace("',),('",",").replace("('","").replace("',)","")
   cur.execute(tstampcol)
   resulttstampcol = cur.fetchall()
   jointstampcol = ",".join(str(x) for x in resulttstampcol)
   replacetstampcol=jointstampcol.replace("('","to_char(").replace("',)",",'DD-MON-YY HH:MI:SSXFF AM')")
   cur.execute(blobcol)
   resultblobcol = cur.fetchall()
   joinblobcol = ",".join(str(x) for x in resultblobcol)
   replaceblobcol=joinblobcol.replace("('","utl_raw.cast_to_varchar2(dbms_lob.substr(").replace("',)","))")
   castallcol1=replaceallcol+","+replacetstampcol+","+replaceblobcol
   castallcol2=castallcol1.replace(",,",",")
   castallcol = re.sub(',$', '',castallcol2)
   print castallcol
   logging.info('Castallcolumn:%s',castallcol)
   where_clause="where upper(table_name)='%s' and upper(owner)='%s'" %(TABLE,user)
   print where_clause
   logging.info('where clause for all_tab_columns:%s',where_clause)
  else:
   selcol1 = dcol.replace(",","','")
   selcol = """select COLUMN_NAME from all_tab_columns where TABLE_NAME = '%s' and COLUMN_NAME in ('%s') and DATA_TYPE not like '%s'""" % (TABLE,selcol1,tstampdt)
   tstampcol = """select COLUMN_NAME from all_tab_columns where TABLE_NAME = '%s' and COLUMN_NAME in ('%s') and DATA_TYPE like '%s'""" % (TABLE,selcol1,tstampdt)
   blobcol = """select COLUMN_NAME from all_tab_columns where TABLE_NAME = '%s' and DATA_TYPE like 'BLOB'""" % (TABLE)
   connection = cx_Oracle.connect(constr)
   cur = connection.cursor()
   cur.execute(selcol)
   resultallcol = cur.fetchall()
   joinallcol = ",".join(str(x) for x in resultallcol)
   replaceallcol=joinallcol.replace("',),('",",").replace("('","").replace("',)","")
   cur.execute(tstampcol)
   resulttstampcol = cur.fetchall()
   jointstampcol = ",".join(str(x) for x in resulttstampcol)
   replacetstampcol=jointstampcol.replace("('","to_char(").replace("',)",",'DD-MON-YY HH:MI:SSXFF AM')")
   cur.execute(blobcol)
   resultblobcol = cur.fetchall()
   joinblobcol = ",".join(str(x) for x in resultblobcol)
   replaceblobcol=joinblobcol.replace("('","utl_raw.cast_to_varchar2(dbms_lob.substr(").replace("',)","))")
   castallcol1=replaceallcol+","+replacetstampcol+","+replaceblobcol
   castallcol2=castallcol1.replace(",,",",")
   castallcol = re.sub(',$', '',castallcol2)
   ncallcol1=joinallcol+","+jointstampcol+","+joinblobcol
   ncallcol=ncallcol1.replace(",,",",").replace(",),(",",").replace(",)",")")
   uallcol=ncallcol.upper()
   print castallcol
   logging.info('Castallcolumn:%s',castallcol)
   print uallcol
   where_clause="where upper(table_name)='%s' and upper(owner)='%s' and upper(column_name) in %s" %(TABLE,user,uallcol)
   print where_clause
   logging.info('allcolumn:%s',where_clause)
  
  tgt=extloc+dcc+"/"+feed+"/"+vdte+"/"+runid+"/data/"
  tgtnm=feed+"_"+dtbl+"_"+runid+"_"+dtm+"_"
  metatgt=extloc+dcc+"/"+feed+"/"+vdte+"/"+runid+"/metadata/"
  directory = os.path.dirname(tgt)
  metadirectory = os.path.dirname(metatgt)
  logging.info('directory:%s',directory)
  logging.info('metadirectory:%s',metadirectory)
  if not os.path.exists(os.path.dirname(tgt)):
   print "creating directory"
   logging.info('Creating target directory')
   os.makedirs(directory)
  print "directory available"
  if not os.path.exists(os.path.dirname(metatgt)):
   print "creating directory"
   logging.info('Creating metatarget directory')
   os.makedirs(metadirectory)
  print "Metadata directory available"
  logging.info('Target directory already available')
  sqll = []
  querystring = """SELECT a.partition_name,
  CASE
    WHEN ceil(SUM(bytes)/1024/1024/100) > ROUND(num_rows/10000000)
    THEN ceil(SUM(bytes)/1024/1024/100)
    ELSE ROUND(num_rows /10000000) END bckts
FROM dba_tab_partitions a,
  dba_segments b
WHERE a.table_owner  = 'user'
AND a.table_name     = 'TABLE'
AND a.table_name     = b.segment_name
AND a.table_owner    = b.owner
AND a.partition_name = b.partition_name
GROUP BY a.table_name,
  a.partition_name,
  num_rows"""

  querystring_nonpart="""SELECT
   CASE
    WHEN ceil(SUM(bytes)/1024/1024/100) > ROUND(num_rows/10000000)
    THEN ceil(SUM(bytes)/1024/1024/100)
    ELSE ROUND(num_rows /10000000) END bckts
FROM dba_segments a,
  dba_tables b
WHERE a.owner      = 'user'
AND a.segment_name = 'TABLE'
AND a.owner        = b.owner
AND a.segment_name = b.table_name
GROUP BY b.table_name,
  num_rows"""

##################### Get volume of source table & divide into multiple blocks #################

  def check_part(constr,user,table):

    connection = cx_Oracle.connect(constr)
    cur = connection.cursor()
    sql="select 1 from dba_tab_partitions where table_owner='%s' and table_name = '%s'"%(user,table)
    cur.execute(sql)
    result = cur.fetchall()
    print len(result)
    return len(result)


  def get_rid_rng(constr,user,table,blk,partition=None):

    connection = cx_Oracle.connect(constr)
    master_sql = """select 
  min(rowid) from_rowid, 
  max(rowid) to_rowid  
from (
  select rowid,
    ntile(blk) over(order by rowid) bucket 
  from user.TABLE PARTITION
)
group by bucket
order by bucket"""

    master_sql = master_sql.replace("user",user)
    master_sql = master_sql.replace("TABLE",TABLE)
    if partition:
        part = "PARTITION (%s)"%(partition)
    else:
        part = ""
    master_sql = master_sql.replace("PARTITION",part)
    master_sql = master_sql.replace("blk",str(blk))

    print master_sql
    logging.debug('master_sql = %s',master_sql)
    db = cx_Oracle.connect(dusr,spassw,host)
    cur = db.cursor()
    cur.execute(master_sql)
    data = cur.fetchall()
    cur.close
    db.close()
    return data

  def get_sql_list(castallcol,user,TABLE,dwhr,data,sql_pretext,partition=None):


    for rows,idx in zip(data,range(len(data))):
        if partition:
            sqll.append((idx,sql_pretext%(castallcol,user,TABLE,partition,dwhr,rows[0],rows[1])))
        else:
            sqll.append((idx,sql_pretext%(castallcol,user,TABLE,dwhr,rows[0],rows[1])))

 
  def get_sql_list_incr(castallcol,user,TABLE,dwhr,data,sql_pretext,partition=None):
    ival=get_incr_var()
    incrvalue = ival[0][0]
    logging.info('Incrval: %s',ival)
    logging.info('Incrvalue: %s',incrvalue)
    for rows,idx in zip(data,range(len(data))):
        if partition:
            sqll.append((idx,sql_pretext%(castallcol,user,TABLE,partition,dwhr,dincl,incrvalue,rows[0],rows[1])))
        else:
            sqll.append((idx,sql_pretext%(castallcol,user,TABLE,dwhr,dincl,incrvalue,rows[0],rows[1])))

  def get_incr_var():
    incr_sql = """select INCR_VALUE from juniper_ext_table_status where run_id =
(select max(a.run_id)from juniper_ext_table_status a, JUNIPER_EXT_NIFI_STATUS b
where a.feed_id = dfid and b.STATUS = 'SUCCESS'
and a.feed_id = b.feed_id and a.project_sequence = b.project_sequence ) and table_name = 'dtbl' and rownum =1"""

    incr_sql = incr_sql.replace("dfid",str(dfid)).replace('dtbl',dtbl)
    connection = cx_Oracle.connect(constrmeta)
    cur = connection.cursor()
    cur.execute(incr_sql)
    data1 = cur.fetchall()
    print incr_sql
    return data1

################################ Do process partitioned tables #################################

  connection = cx_Oracle.connect(constr)
  cur = connection.cursor()

  if check_part(constr,user,TABLE) > 0:
    logging.info('Source table is Partitioned, grouping data based on table parition')
    querystring = querystring.replace("user",user).replace("TABLE",TABLE)
    logging.info('%s',querystring)
    cur.execute(querystring)
    result = cur.fetchall()

    for i,j in result:

        prtname = i
        blk = j
        if dtyp == 'full':
         sql_pretext="select %s from %s.%s PARTITION (%s) where %s and rowid between chartorowid('%s') and chartorowid('%s')"
         data=get_rid_rng(constr,user,TABLE,blk,prtname)
         get_sql_list(castallcol,user,TABLE,dwhr,data,sql_pretext,prtname)
        else:
         data1=get_incr_var()
         if data1 == []:
          print "Incremental field value is null,so extracting as full"
          logging.debug('Incremental field value is null,so extracting as full')
          sql_pretext="select %s from %s.%s PARTITION (%s) where %s and rowid between chartorowid('%s') and chartorowid('%s')"
          data=get_rid_rng(constr,user,TABLE,blk,prtname)
          get_sql_list(castallcol,user,TABLE,dwhr,data,sql_pretext,prtname)
         else:
          sql_pretext="select %s from %s.%s PARTITION (%s) where %s and %s>'%s' and rowid between chartorowid('%s') and chartorowid('%s')"
          data=get_rid_rng(constr,user,TABLE,blk,prtname)
          get_sql_list_incr(castallcol,user,TABLE,dwhr,data,sql_pretext,prtname)

################################ Do process unpartitioned tables ###############################

  else:
    print "IN NON PART"
    logging.info('Source table is not Partitioned, grouping data based on volume')
    querystring_nonpart = querystring_nonpart.replace("user",user)
    querystring_nonpart = querystring_nonpart.replace("TABLE",TABLE)

    print querystring_nonpart
    logging.info('%s',querystring_nonpart)
    cur.execute(querystring_nonpart)
    blk = cur.fetchall()
    print blk
    if blk == []:
     print "skipping %s as the table is empty" %TABLE
     logging.info('skipping %s for extraction as the table is empty',TABLE)
     tblcnt = 0
     Conversion_process(dtbl,tblcnt,where_clause,constr,dwhr)
     continue
    else: 
     print blk[0][0]
     logging.info('Data is grouped into %s blocks',blk[0][0])
     if dtyp == 'full':
      sql_pretext="select %s from %s.%s where %s and rowid between chartorowid('%s') and chartorowid('%s')"
      data=get_rid_rng(constr,user,TABLE,blk[0][0])
      get_sql_list(castallcol,user,TABLE,dwhr,data,sql_pretext)
     else:
      data1=get_incr_var()
      print data1
      if data1 == []:
       print "Incremental field value is null,so extracting as full"
       logging.debug('Incremental field value is null,so extracting as full')
       sql_pretext="select %s from %s.%s where %s and rowid between chartorowid('%s') and chartorowid('%s')"
       data=get_rid_rng(constr,user,TABLE,blk[0][0])
       get_sql_list(castallcol,user,TABLE,dwhr,data,sql_pretext)
      else:
       sql_pretext="select %s from %s.%s where %s and %s>'%s' and rowid between chartorowid('%s') and chartorowid('%s')"
       data=get_rid_rng(constr,user,TABLE,blk[0][0])
       get_sql_list_incr(castallcol,user,TABLE,dwhr,data,sql_pretext)


    print sqll
    logging.info('Sqll = %s',sqll)
    if sqll == []:
     print "No data in table '%s' to extract" %TABLE
     logging.debug('No data in table %s to extract',TABLE)
     tblcnt = 0
     Conversion_process(dtbl,tblcnt,where_clause,constr,dwhr)
     continue
     print "sqll"

  def f(sql):
    enclosure="\xe1\xba\xa4"
    logging.info('Extraction commencing for : %s',TABLE)
    c = '%s./extractorC userid="%s" sqlstmt="%s" arraysize=5000 delimiter="," enclosure="%s" > %s%s%s.csv'%(baseloc,str(constr),sql[1],enclosure,tgt,tgtnm,sql[0])
    print c
    logging.info('Command: %s',c)
    result = subprocess.Popen(c,shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT).stdout.read()
    print result
    logging.info('Execute result: %s',result)
    logging.info('Extraction completed for : %s',TABLE)
    return "thread complete"

  def num_lines_in_file(dtbl):
    logging.info('counting num of line in file')
    connection = cx_Oracle.connect(constr)
    cur = connection.cursor()
    tbl_cnt = """select count(*) from %s""" % (dtbl)
    print tbl_cnt
    cur.execute(tbl_cnt)
    tblcnt = cur.fetchall()
    return tblcnt[0][0]
   # countoflines='ls -l %s|wc -l` -eq 1 ]]; then echo `wc -l %s`; else echo `wc -l %s| grep total`; fi'%(fpath,fpath,fpath)
   # return int(subprocess.check_output('if [[ `ls -l %s|wc -l` -eq 1 ]]; then echo `wc -l %s`; else echo `wc -l %s| grep total`; fi' % (fpath,fpath,fpath), shell=True).strip().split()[0])

  def find_files(base, pattern):
###################### Return list of files matching pattern in base folder ####################
    return [n for n in fnmatch.filter(os.listdir(base), pattern) if
        os.path.isfile(os.path.join(base, n))]

  if __name__ == '__main__':
    p = Pool(prll)
    print("Initiating %s parallel sessions for data extraction" % (prll))
    logging.info('Initiating %s parallel sessions for data extraction',prll)
    #print(p.map(f, sqll))
    logging.info('Main: %s',(p.map(f, sqll)))
  end = datetime.datetime.now()
  tgt=extloc+dcc+"/"+feed+"/"+vdte+"/"+runid+"/data/"
  tgtnm=feed+"_"+dtbl+"_"+runid+"_"+dtm+"_"
#  tblcnt = num_lines_in_file(tgt+tgtnm+'*.csv')
  tblcnt = num_lines_in_file(dtbl)
  logging.info('num line in table %s %s',dtbl, tblcnt)
  list_files = find_files(tgt, '*.csv')
  logging.info('Listing files for Table %s: %s',TABLE,list_files)
####################### File extraction Success/Failure status #################################
  if list_files == []:
   exests = "FAIL"
  else:
   exests = "SUCCESS"
  logging.info('Extraction Status: %s',exests)
  logging.info('Commencing Json build for: %s',TABLE)
  Conversion_process(dtbl,tblcnt,where_clause,constr,dwhr)   

logging.info('Sucessfully completed avro conversion process - @ %s',end)
