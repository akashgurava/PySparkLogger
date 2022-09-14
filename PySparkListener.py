#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import base64
import codecs
import csv
import gzip
import json
import re
import subprocess
import sys
import traceback
from collections import Counter
from datetime import datetime
from functools import reduce
from io import BytesIO
import numpy as np

import boto3
import botocore.session
from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

sys.path.insert(0, 'xdp.zip')
sys.path.insert(0, 'xdp.zip/logger/')
sys.path.insert(0, 'xdp.zip/shared/')
sys.path.insert(0, 'xdp.zip/fileingestion/')
sys.path.insert(0, 'xdp.zip/fileingestion/bin/')

from logger import create_logger, get_logger, xdpLogger

from fileingestion.config import app_config
from fileingestion.util import get_running_file_details
from fileingestion.util import frmexceptions, frmspark
from fileingestion.util import utilities as utility
from fileingestion.bin.extract_good_data import PublishToStage
from fileingestion.bin.fi_datatype_handling import *
from fileingestion.tasks import file_expctd_list_pd, file_checklist_pd, file_schema_info_pd, ref_mapping_pd


# GLOBAL PARAMETERS
snap_dt = ""
file_name = ""
file_path = ""
arg = sys.argv[1]
src_sys_nm, input_file_prefix = utility.extract_Src_FilePrefix(arg)
target_stage_db = utility.get_target_db(src_sys_nm, input_file_prefix)
target_stage_trnf_db = utility.get_trnf_target_db(src_sys_nm, input_file_prefix) #--- WMIT-11391

try:
    DQ_FLAG = int(sys.argv[2])
except Exception as e:
    print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of : {e}")
    xdpLogger("xDP-WAR-041",comment=f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of : {e}")
    DQ_FLAG = 1

# DQ_FLAG = 0 ==> NO DQ step
#             ==> This code handles file_level_checks, datatype_handling and extract_good_data
# DQ_FLAG = 1 ==> DQ driver needs to be run
#             ==> This code handles file_level_checks, datatype_handling.
#             ==> DQdriver and Extract_good_data should be run next.

run_id = ""
today_1 = utility.dateToday()
if src_sys_nm is None or src_sys_nm == '':
    file_base_loc = app_config.file_proc_loc+input_file_prefix+"/"
else:
    file_base_loc = app_config.file_proc_loc+src_sys_nm+"-"+input_file_prefix+"/"
spark = utility.get_spark()
sc = spark.sparkContext

try:
    obf_flag = app_config.obfuscation_flag == True
except:
    obf_flag = False


def create_S3_folder(temp_tbnm):
    get_logger()
    xdpLogger('xDP-INF-001')
    session = botocore.session.get_session()
    client = session.create_client('s3')
    bucket_name = app_config.bucket_name
    temp_loc_split = app_config.temp_table_loc.split('/')
    len_split_loc = len(temp_loc_split)
    abs_path_split = temp_loc_split[3:len_split_loc]
    abs_path_split = [x for x in abs_path_split if x]
    folder_name = "/".join(abs_path_split)
    suffix = folder_name
    response = client.put_object(
        Bucket = bucket_name,
        Key = suffix + "/" + temp_tbnm + "/"
    )

def create_table(stg_db,table,col_info,input_regex):
    """Description: This function creates hive table corresponding to the view passed as parameter
    :param list - database,table name,column_info,input_regex
    :return
    """
    get_logger()
    xdpLogger('xDP-INF-001')
    tablename = "{}".format(target_stage_db) + "." + "{}".format(table)
    temp_csv_folder = "temp_file.csv"
    create_query = """CREATE EXTERNAL TABLE IF NOT EXISTS {} ({})
                ROW FORMAT SERDE
                  'org.apache.hadoop.hive.serde2.RegexSerDe'
                WITH SERDEPROPERTIES (
                  'input.regex'='{}')
                STORED AS INPUTFORMAT
                  'org.apache.hadoop.mapred.TextInputFormat'
                OUTPUTFORMAT
                  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                LOCATION
                  '{}{}/{}'
                 """.format(tablename,col_info,input_regex,app_config.temp_table_loc,tablename,temp_csv_folder)
    xdpLogger('xDP-INF-111',comment="Table: {}".format(tablename))
    frmspark.execute_query(create_query)
    xdpLogger('xDP-INF-021',comment="Table created: {}".format(tablename))

def remove_all_hex(col_name,hex_list):
    """
    REMOVE_ALL_HEX FUNCTION WILL REMOVE THE HEX_VALUES PROVIDED IN THE HEX_LIST BY COMBINING THEM INTO A COMMON REGEX
    """
    regexp = "|".join(hex_val for hex_val in hex_list)
    return F.regexp_replace(col_name, regexp, "")


def mapping_details(group_name):
    get_logger()
    xdpLogger('xDP-INF-001')
    fetch_mapping_query = "select key1,val1 from {table} where lower(group_name) = '{group}' ".format(table = app_config.mapping_table,group = group_name.lower())
    mapping_details_df = frmspark.execute_query_ctrl_tbl_api(fetch_mapping_query) #Changes for Jira::WMIT-5794 | to reduce number of partitions in dataframe
    mapping_details_rdd = mapping_details_df.collect()
    return mapping_details_rdd

def rename_cols(rename_df,mapping_details_rdd):
    get_logger()
    xdpLogger('xDP-INF-001')
    for column in rename_df.columns:
        old_column = column
        new_column = column
        for values in mapping_details_rdd:
            intermediate_column = new_column.replace(values[0],values[1])
            new_column = intermediate_column
        rename_df = rename_df.withColumnRenamed(old_column, new_column)
    return rename_df

def handleDouble(column_value):
    get_logger()
    xdpLogger('xDP-INF-001')
    if column_value is None:
        return None
    elif column_value.startswith("\"")&column_value.endswith("\""):
        return column_value[1:-1]
    else:
        return column_value

def extract_data_records(df_input,df_header_count,df_tail_count):
    xdpLogger('xDP-INF-111',comment='extract_data_records function started')
    rdd_data = df_input.rdd.map(list)
    # handling headers
    if df_header_count == 0:
        data_headers = []
    elif df_header_count ==1 :
        data_headers = rdd_data.take(1)
    elif df_header_count ==2 :
        data_headers = rdd_data.take(2)
    else:
        xdpLogger('xDP-ERR-003',comment="Unsupported header_count {df_header_count}".format(df_header_count=df_header_count))
        spark.stop()
        sys.exit(1)
    #handling tail
    if df_tail_count == 0:
        last_row = []
        last_row_na = []
    elif df_tail_count == 1:
        total_rows = rdd_data.count()
        last_row_rdd = rdd_data.zipWithIndex().filter(lambda x:  x[1]==total_rows -1)
        last_row = last_row_rdd.first()[0][:]
        last_row_na = [None if x=='' else x for x in last_row]
    else:
        xdpLogger('xDP-ERR-003',comment="Unsupported tail_count {df_tail_count}".format(df_tail_count=df_tail_count))
        value_list = [app_config.file_ingestion_detail,str(snap_dt),run_id,str(file_name),src_name,input_file_prefix,'NA','Filelevelcheck',"Unsupported df_tail_count {df_tail_count}".format(df_tail_count=df_tail_count), 'Failed',utility.timeStamp(),today_1]
        spark.stop()
        sys.exit(1)
    xdpLogger('xDP-INF-111',comment='data_headers : {} function started'.format(data_headers))
    xdpLogger('xDP-INF-111',comment='last_row_na : {} function started'.format(last_row_na))

    rdd_data = rdd_data.filter(lambda line: line not in data_headers and len(line)>0 and line !=last_row and line !=last_row_na)
    #converting rdd to df
    total_cols =len(df_input.columns)
    inp_f_lst = [StructField('col_'+str(val), StringType(), True) for val in range(total_cols)]
    inp_schema = StructType(inp_f_lst)
    write_df = rdd_data.toDF(inp_schema)
    return write_df

def file_level_check(file_expctd_df, file_checklist_df, file_schema_info_df, mapping_df, is_fw_file,is_multiline,is_dataclean,csv_filename,delimiter_value,input_file_prefix,snap_dt,run_id,src_name,file_name,meta_data_df,rowcountvariance_df, df, bad_rows_cnt, df_header_count,**kwargs):

    """FILE_LEVEL_CHECK FUNCTION READS THE DQ_FILE_CHECK_CTL TABLE AND EXECUTES THE TESTS MENTIONED IN  THE TABLE
    CAPABLE OF HANDLING FUNCTIONS WITH AND WITHOUT ARGUMRNTS """
    try:
        get_logger()
        xdpLogger('xDP-INF-001')

        success_list=[]
        failure_list=[]
        rules=[]
        check=''
        checkAll=True
        file_content_format = kwargs.get('file_content_format','')
        kwargs.pop('file_content_format',None)
        df_tail_count = kwargs.get('df_tail_count','')
        kwargs.pop('df_tail_count',None)
        last_row=kwargs.get('last_row','')
        kwargs.pop('last_row', None)
        file_content_dict = kwargs.get('file_content_dict','')
        kwargs.pop('file_content_dict', None)
        data_count = kwargs.get('data_count','')
        kwargs.pop('data_count',None)

        
        file_expctd_df_filtered = file_expctd_df.loc[(file_expctd_df["file_prefix_nm"].str.lower() == input_file_prefix.lower()) & 
                                                        (file_expctd_df["src_sys_nm"] == src_name)]

        fetch_trgt_tbl = file_expctd_df_filtered
        fetch_trgt_tbl.loc[:,'trgt_tbl'] = np.select(file_expctd_df_filtered['trgt_tbl_nm'].isnull(),file_expctd_df_filtered['file_prefix_nm'],file_expctd_df_filtered['trgt_tbl_nm'])
        trgt_tbl = fetch_trgt_tbl['trgt_tbl'].unique()[0]

        parquet_table = target_stage_trnf_db+"."+trgt_tbl+"_"+"parquet" #------ WMIT-11391
        table_name=parquet_table
        
        file_checklist_df_filtered = file_checklist_df.loc[file_checklist_df['file_prefix_nm'] == input_file_prefix]
        file_chck_typ = file_checklist_df_filtered.sort_values(by = ['priority']).drop_duplicates(['file_chck_typ','arguments','priority'])

        rule=[{'checks':x['file_chck_typ'],'arguments':x['arguments']} for _,x in file_chck_typ.iterrows()]
        
        for row in rule:
            rules.append([row['checks'],row['arguments']])
        xdpLogger('xDP-INF-111',comment="File Level Check Rules,Rules:{}".format(rules))
        for file_check in rules :
            #print("for file check in rules")
            if str(file_check[0]).lower() in ['rowcountcheck','filecontentcheck','sumcheckvalidation','rowcountvariancecheck','integritycheck'] and not(isinstance(last_row,list)) and is_fw_file == False:
                xdpLogger('xDP-DBG-001',comment="Last row required for ,Rule:{}".format(str(file_check[0])))
                last_row=last_row.collect()
            xdpLogger('xDP-DBG-001',comment="Rule Check Iteration ,Rule:{}".format(str(file_check[0])))
            xdpLogger('xDP-DBG-001',comment="File Check,File Check :{}".format(str(file_check[1])))
            xdpLogger('xDP-DBG-001',comment="{}".format(str(file_check[1])))
            if (file_check[1] == None or str(file_check[1])==''):
                xdpLogger('xDP-INF-111',comment="NO ARGUMENTS AVAILABLE FOR THE CHECK FUNCTION")
                df_pass=df
            else:
                df_pass=str(file_check[1])
                xdpLogger('xDP-INF-111',comment="ARGUMENTS AVAILABLE FOR THE CHECK FUNCTION")
            if checkAll!=False:
                if last_row == '':
                    check,check_f = getattr(utility,str(file_check[0]).lower())(is_fw_file,df_pass,file_name,snap_dt,run_id,input_file_prefix,src_name,meta_data_df,df,rowcountvariance_df, bad_count = bad_rows_cnt,file_content_dict = file_content_dict,data_count = data_count,**kwargs)
                else:
                    check,check_f = getattr(utility,str(file_check[0]).lower())(is_fw_file,df_pass,file_name,snap_dt,run_id,input_file_prefix,src_name,meta_data_df,df,rowcountvariance_df, last_row = last_row, bad_count = bad_rows_cnt, file_content_dict = file_content_dict,data_count = data_count,**kwargs)

                success_list,failure_list=logEntry(check,check_f,src_name,input_file_prefix,success_list,failure_list)
                print(success_list)
                print("check here")
                print(failure_list)
                if ('Waiting' in (item[9] for item in success_list)):
                    print("Waiting found here")
                    writeStatus2FileIngestion(success_list+failure_list)
                    print("Waiting - Stopping spark and sys exit with 1")
                    spark.stop()
                    sys.exit(1)
                    print("Waiting not-stopped")
                else:
                    print("Waiting or IC_skipped not found")

                if check!=False:
                    checkAll=True
                    xdpLogger('xDP-DBG-001',comment="Check has Passed")
                else:
                    xdpLogger('xDP-DBG-001',comment="Check has failed")
                    checkAll=False
            else:
                list_skpd = (app_config.file_ingestion_detail,snap_dt,run_id,file_name,src_name,input_file_prefix,'NA', 'Filelevelcheck',str(file_check[0]), 'Skipped',utility.timeStamp(),today_1)
                failure_list.append(list_skpd)
                xdpLogger('xDP-INF-111',comment="CHECK SKIPPED")
            xdpLogger('xDP-INF-111',comment="Passed Check Name,Name:{}".format(check))
            xdpLogger('xDP-INF-111',comment="Failed Check Name,Name:{}".format(check_f))

        if checkAll:
            if is_fw_file:
                temp_tb = input_file_prefix + "_temp_csv"
                drop_query_temp_mid = "DROP TABLE IF EXISTS {}".format(target_stage_db) + "." + "{}".format(temp_tb)
                frmspark.execute_query(drop_query_temp_mid)
                try:
                    table_folder = app_config.temp_table_loc + target_stage_db+"."+temp_tb + "/"
                    a = subprocess.call(['aws','s3','rm',table_folder,'--recursive','--exclude',''])
                    if a == 0:
                        xdpLogger('xDP-INF-111',comment="Temporary location cleaned, Table is {}".format(temp_tb))
                    else:
                        xdpLogger('xDP-ERR-001',comment="Error while deleting temporary location")
                        xdpLogger('xDP-INF-999')
                        sys.exit(0)

                except CapturedException as exp:
                    xdpLogger('xDP-ERR-004')
                    sys.exit(0)
                temp_write_path = app_config.temp_table_loc + target_stage_db + "." + temp_tb + "/temp_file.csv"
                df.write.format("csv").option("ignoreTrailingWhiteSpace",False).option("ignoreLeadingWhiteSpace",False).save(temp_write_path)

                file_schema_info_df_filtered = file_schema_info_df.loc[file_schema_info_df['file_prefix_nm'].str.lower() == input_file_prefix.lower()]
                col_length_df = file_schema_info_df_filtered.sort_values(by = ['col_seq'])[['col_length']]

                col_length_list = [int(x['col_length']) for _,x in col_length_df.iterrows()]
                
                input_regex = ""
                for col_len in col_length_list:
                    input_regex = input_regex + "(.{" + str(col_len) + "})"
                col_list = kwargs.get('col_list','')
                table_col_list = ""
                for col in col_list:
                     table_col_list = table_col_list + "`" + col + "` " + "string,"
                table_col_list = table_col_list[:-1]
                temp_tbnm_with_db = target_stage_db+"."+temp_tb
                create_S3_folder(temp_tbnm_with_db)
                create_table(target_stage_db,temp_tb,table_col_list,input_regex)

                write_df = frmspark.execute_query("select * from {}".format(temp_tbnm_with_db))
                write_df = write_df.na.drop(how='all')

            elif is_multiline == True and is_dataclean == False:
                print ("multiLine True and Dataclean False")
                multiLine_json_obj = json.loads(mapping_df.filter(F.col('group_name')=='listOfMultilineFiles').select('val1').rdd.flatMap(lambda x: x).collect()[0])
                inp_f_lst = [StructField('col_'+str(val), StringType(), True) for val in range(multiLine_json_obj['file_prefix_nm'][input_file_prefix]['total_column'])]
                inp_schema = StructType(inp_f_lst)
                
                df_input = frmspark.execute_read(infer_schema=False, schema=inp_schema, header=False, quote=None,escape=multiLine_json_obj['file_prefix_nm'][input_file_prefix]['escape'], csv=csv_filename, delim=delimiter_value,multiLine=True)
                if '_no' in file_content_format:
                    print("no_prefix file")
                    write_df = extract_data_records(df_input,df_header_count,df_tail_count)
                elif ((df_header_count>0 and df.take(1)[0][0]=='D') or (df_header_count==0 and df.take(1)[0][0]=='D')):
                    df_input = df_input.filter(df_input[0] == 'D')
                    write_df=df_input.drop(df_input[0])
                    #write_df.show()
                elif (df_header_count>0 and df.take(1)[0][0]!='D'):
                    rdd_data = df_input.rdd.map(list)
                    header_data = rdd_data.first()
                    last_row_na = [None if x=='' else x for x in last_row]
                    rdd_data = rdd_data.filter(lambda line: line != header_data and line !=last_row and line !=last_row_na)
                    write_df = rdd_data.toDF(inp_schema)
                else:
                    write_df=df
                write_df = write_df.na.drop(how='all')
            elif is_multiline == True and is_dataclean == True:
                print ("in multiLine and data Clean")
                df_input = datacleaning(csv_filename,src_name,input_file_prefix,delimiter_value,is_multiline)
                if '_no' in file_content_format:
                    print("no_prefix file")
                    write_df = extract_data_records(df_input,df_header_count,df_tail_count)
                elif ((df_header_count>0 and df.take(1)[0][0]=='D') or (df_header_count==0 and df.take(1)[0][0]=='D')):
                    df_input = df_input.filter(df_input[0] == 'D')
                    write_df=df_input.drop(df_input[0])
                    #write_df.show()
                elif (df_header_count>0 and df.take(1)[0][0]!='D'):
                    rdd_data = df_input.rdd.map(list)
                    header_data = rdd_data.first()
                    last_row_na = [None if x=='' else x for x in last_row]
                    rdd_data = rdd_data.filter(lambda line: line != header_data and line !=last_row and line !=last_row_na)
                    write_df = rdd_data.toDF(inp_schema)
                else:
                    write_df=df
                write_df = write_df.na.drop(how='all')
                
            else:
                if ((df_header_count>0 and df.take(1)[0][0]=='D') or (df_header_count==0 and df.take(1)[0][0]=='D')):
                    write_df=df.drop(df[0])
                    write_df.show()
                else:
                    write_df=df
                write_df = write_df.na.drop(how='all')

            write_df = write_df.withColumn('snap_dt',lit(snap_dt))
            write_df = write_df.withColumn('run_id',lit(run_id))
            
            intra_day = file_expctd_df.loc[(file_expctd_df['file_prefix_nm'].str.lower() == input_file_prefix.lower()) &
                                            (file_expctd_df['trgt_tbl_nm'] == trgt_tbl)]
            intra_day.loc[:,'flag'] = np.select(intra_day['intraday_flag'].isnull(), 'n', intra_day['intraday_flag'].str.lower()) 
            intraday_flag = intra_day['flag'].unique()[0]

            if intraday_flag == 'y':
                snap_dt_ts = meta_data_df.select("meta_data_details").filter(meta_data_df.snap_dt == snap_dt).filter(meta_data_df.file_nm == file_name).first()[0]
                snap_dt_ts = str(snap_dt_ts)
                if "snap_dt_timestamp" in snap_dt_ts:
                    value = snap_dt_ts.split(",")[2].split(":",1)[1]
                else:
                    xdpLogger("xDP-WAR-041",comment="snap_dt_timestamp not found for intraday load, process will terminate")
                    spark.stop()
                    sys.exit(1)
                write_df = write_df.withColumn('snap_dt_ts',lit(value))

            if (obf_flag==True):
                from fileingestion.config import obf_app_config
                from fileingestion.util import file_ingestion_obf
                xdpLogger('xDP-INF-017',comment="Obfuscation required as Flag is set to True.")
                key=get_Obfs_Key(spark)
                udf_obfuscate = spark.udf.register("udf_obfuscate",vernam_encrypt)   
                udf_deobfuscate = spark.udf.register("udf_deobfuscate",vernam_decrypt)
                colm_list = column_list(input_file_prefix,[]) 
                colm_list.append('snap_dt')
                colm_list.append('run_id')
                if intraday_flag == 'y':
                    colm_list.append('snap_dt_ts')
                write_df=write_df.toDF(*colm_list)
                #write_df.createOrReplaceTempView("write_df")
                #write_df.show()
                #query = "insert overwrite table {} partition(snap_dt, run_id) select * from write_df".format(table_name)

                latest_config_snap_dt=None
                latest_rest_snap_dt=None
                try:
                    write_df,latest_config_snap_dt, latest_rest_snap_dt = file_ingestion_obf.run_file_ingestion_obfuscation(spark, write_df, target_stage_db, trgt_tbl,src_sys_nm,snap_dt, run_id, file_name, input_file_prefix)
                except Exception as exp:
                    xdpLogger("xDP-ERR-003",comment="Error occured while obfuscating the df.")
                    xdpLogger("XDPEXP",exp) 
                    raise exp
                xdpLogger('xDP-INF-017',comment="Insert overwrite table {}".format(table_name))
                #HB_CHECK is inserting into parquet needed?
                if DQ_FLAG :
                    print("parquet insertion has been removed") #write_df.select(colm_list).write.insertInto(table_name,overwrite = True)
                #frmspark.execute_query(query)

                if(not(latest_config_snap_dt is None or latest_rest_snap_dt is None)):
                
                    try:
                        file_ingestion_obf.write_to_ingestion_details(spark,snap_dt,run_id, file_name, input_file_prefix, parquet_table, 'OBFUSCATION_COMPLETED', '', 'Pass',src_sys_nm)
                        xdpLogger('xDP-INF-111',comment="Write to file ingestion details table successful for Obfuscation Completed in partitioned table "+ parquet_table +" for snap_dt "+snap_dt+" and run id "+ str(run_id))
                    except CapturedException as exp:
                        print(f"{type(exp).__name__} at line {exp.__traceback__.tb_lineno} of : {exp}")
                        xdpLogger("xDP-WAR-041",comment=f"{type(exp).__name__} at line {exp.__traceback__.tb_lineno} of : {exp}")
                        xdpLogger("xDP-ERR-003",comment="Error occured while making entry in ingestion details table for Obfuscation Completed in partitioned table "+ parquet_table +" for snap_dt "+snap_dt+" and run id "+ str(run_id))
                        xdpLogger("XDPEXP",exp) 
                        sys.exit(1)   
                    cast_snap_dt=str(datetime.strptime(snap_dt, '%Y%m%d').date())
                    try:
                        file_info=str(snap_dt)+";"+str(run_id)+";"+str(file_name)
                        query = "insert into "+obf_app_config.log_table + " values ('"+parquet_table.lower()+"','snap_dt','"+cast_snap_dt+"','"+latest_config_snap_dt+"','"+latest_rest_snap_dt+"','"+utility.dateToday()+"','OBFUSCATION_COMPLETED','"+file_info+"','"+utility.timeStamp()+"')"
                        frmspark.execute_query(query)
                        xdpLogger('xDP-INF-111',comment="Write to obfuscation info table successful for Obfuscation COMPLETED in partitioned table "+ parquet_table +" for snap_dt "+snap_dt)
                        print ("Write to obfuscation info table successful for Obfuscation COMPLETED in partitioned table "+ parquet_table +" for snap_dt "+snap_dt)
                    except CapturedException as exp:
                        print(f"{type(exp).__name__} at line {exp.__traceback__.tb_lineno} of : {exp}")
                        xdpLogger("xDP-WAR-041",comment=f"{type(exp).__name__} at line {exp.__traceback__.tb_lineno} of : {exp}")
                        xdpLogger("xDP-ERR-003",comment="Error occured while making entry in obfuscation info table for Obfuscation COMPLETED in partitioned table "+ parquet_table +" for snap_dt "+snap_dt)
                        print ("Error occured while making entry in obfuscation info table for Obfuscation COMPLETED in partitioned table "+ parquet_table +" for snap_dt "+snap_dt)
                        xdpLogger("XDPEXP",exp)
                        sys.exit(1)
            else:
                xdpLogger('xDP-INF-017',comment="Obfuscation not required as Flag is not set to True.")
                xdpLogger('xDP-INF-017',comment="Insert overwrite table {}".format(table_name))
                try :
                    #HB_CHECK is inserting into parquet needed?
                    if DQ_FLAG :
                        print("parquet insertion has been removed") #write_df.write.insertInto(table_name,overwrite = True)
                except Exception as e :
                    print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of : {e}")
                    xdpLogger("xDP-WAR-041",comment=f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of : {e}")
                    xdpLogger("xDP-ERR-001",comment="Error occured while inserting into  parquet_table -- {}".format(e))
                    spark.stop()
                    sys.exit(1)
                #frmspark.execute_query(query)

            print("After executing the query")
            list_1= (app_config.file_ingestion_detail,snap_dt,run_id,file_name,src_name,input_file_prefix,'NA','Filelevelcheck', 'Parquetconversion', 'Pass',utility.timeStamp(),today_1)
            success_list.append(list_1) 
        else:
            xdpLogger('xDP-WAR-041',comment="File Level Checks did not pass, process will terminate")
            writeStatus2FileIngestion(success_list+failure_list) #Bug fix status not being logged in file ingestion table.
            return
        print("Running fine")
        # writeStatus2FileIngestion(success_list+failure_list)
        xdpLogger('xDP-DBG-001',comment="Table Name ,Table Name :{}".format(table_name))
        xdpLogger('xDP-INF-111',comment="RESULT of rowcount/schema_validation ,Result of validation Check:{}".format(check))
        # print("col_list ", col_list)
        
        type_casting_df = file_expctd_df_filtered.drop_duplicates(['typecasting_flag'])[['typecasting_flag']]
        type_casting_value = type_casting_df['typecasting_flag'].unique()[0]
        
        type_casting_value = str(type_casting_value)
        # type_casting_value = 'N'

        # print("type_casting_value ",type_casting_value)
        oldColumns = write_df.schema.names
        newColumns = spark.read.table(parquet_table).columns

        actual_data_df = reduce(lambda write_df, idx: write_df.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), write_df)
        part_file_flag=kwargs.get('part_file_flag','')
        kwargs.pop('part_file_flag', None)
        xdpLogger('xDP-INF-111',comment="part_file_flag - {}".format(part_file_flag))
        if type_casting_value == 'Y':
            print("Datatype handling enabled.")
            try:
                dqinp_result_df,dth_entry = DataTypeHandling(file_name, snap_dt, run_id, actual_data_df, DQ_FLAG)
                success_list.append(dth_entry)
                if DQ_FLAG :
                    print("DQ configured.")
                    pass
                else :
                    print("No DQ configured. Publish to Stage start.")
                    try:
                        publish_entry = PublishToStage(file_name, snap_dt, run_id, dqinp_result_df,part_file_flag, intraday_flag)
                        success_list.append(publish_entry)
                        utility.create_fi_done_file(file_name)
                    except Exception as e:
                        # xdpLogger("XDPEXP",e)
                        print("exp" , e)
                        tb = traceback.format_exc()
                        print(tb)
                        xdpLogger("xDP-WAR-041",comment="Exception Occurred, ExtractGoodData failed. Application id : {}, Exception Occurred :{}, traceback : {} ".format(application_id,e,tb))
                        spark.stop()
                        sys.exit(1)
            except Exception as e:
                # xdpLogger("XDPEXP",e)
                print("exp" , e)
                tb = traceback.format_exc()
                print(tb)
                xdpLogger("xDP-WAR-041",comment="Exception Occurred, DataTypeHandling failed. Application id : {}, Exception Occurred :{}, traceback : {} ".format(application_id,e,tb))        
                spark.stop()
                sys.exit(1)
        else:
            print("Datatype handling disabled, directly Publishing to stage.")

            if DQ_FLAG != 0:
                print("DQ configured.")
                dth_entry = copy_df(df,snap_dt,run_id)
                success_list.append(dth_entry)
                pass
            else :
                print("No DQ configured. Publish to Stage start.")
                dth_entry= (app_config.file_ingestion_detail,snap_dt,run_id,file_name,src_name,input_file_prefix,'NA','DataTypeCheck', 'TypeCastingSkipped', 'Pass',utility.timeStamp(),today_1)
                success_list.append(dth_entry)
                try:
                    publish_entry = PublishToStage(file_name, snap_dt, run_id, actual_data_df,part_file_flag,intraday_flag)
                    success_list.append(publish_entry)
                    utility.create_fi_done_file(file_name)
                except Exception as e:
                    xdpLogger("XDPEXP",e)
                    print("exp" , e)
                    tb = traceback.format_exc()
                    print(tb)
                    xdpLogger("xDP-WAR-041",comment="Exception Occurred, ExtractGoodData failed. Application id : {}, Exception Occurred :{}, traceback : {} ".format(application_id,e,tb))        
                    spark.stop()
                    sys.exit(1)

        writeStatus2FileIngestion(success_list+failure_list)
    
    except Exception as e:
        # xdpLogger("XDPEXP",e)
        print("exp" , e)
        tb = traceback.format_exc()
        print(tb)
        xdpLogger("xDP-WAR-041",comment="Exception Occurred, file level checks failed. Application id : {}, Exception Occurred :{}, traceback : {} ".format(application_id,e,tb))                           
        spark.stop()
        sys.exit(1)

def unicode_csv_reader(unicode_csv_data, **kwargs):
    # csv.py doesn't do Unicode; encode temporarily as UTF-8:
    for key,value in kwargs.items():
        if key == 'delimiter':
            delimiter_value = value
    csv.field_size_limit(sys.maxsize)
    csv_reader = csv.reader(remove_null_byte(unicode_csv_data),delimiter = str(delimiter_value), quotechar='"')
    for row in csv_reader:
        # decode UTF-8 back to Unicode, cell by cell:
        yield [cell for cell in row]

def remove_null_byte(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.replace('\0','')

def utf_8_encoder(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.encode('utf-8')

def column_list_fw(column_header_str,prefix):
    column_name_len_query = "select length(trim(column_name)) as col_name_length, col_seq from {} where lower(file_prefix_nm) = '{}' order by col_seq asc".format(app_config.file_schema_info,prefix.lower())
    column_name_len_df = frmspark.execute_query_ctrl_tbl_api(column_name_len_query).orderBy(F.asc("col_seq")).select("col_name_length")
    col_name_rdd = column_name_len_df.collect()
    col_name_len_list = []
    for ele in col_name_rdd:
        col_name_len_list.append(int(ele[0]))
    inc = 0
    col_list = ['H']
    for val in col_name_len_list:
        last = int(inc + val)
        name = column_header_str[inc:last]
        col_list.append(name)
        inc = last
    return col_list

def column_list(input_file_prefix,column_name_list):
    column_name_query = "select column_name , col_seq from {} where lower(file_prefix_nm) = '{}' order by col_seq asc".format(app_config.file_schema_info,input_file_prefix.lower())
    column_name_df = frmspark.execute_query_ctrl_tbl_api(column_name_query).orderBy(F.asc("col_seq")).select("column_name") #Changes for Jira::WMIT-5794 | to reduce number of partitions in dataframe
    column_name_rdd = column_name_df.collect()

    for i in column_name_rdd:
        column_name_list.append(str(i[0]).upper())
    return column_name_list

def column_name_sub(column_header,mapping_values_rdd):
        column_header_list = []
        if len(column_header)!=0:
                for column in column_header:
                        new_column = column
                        for values in mapping_values_rdd:
                                intermediate_column = new_column.replace(values[0],values[1])
                                new_column = intermediate_column
                        column_header_list.append(str(new_column).upper())
        else:
                column_header_list=[]
        #changes for CDL-1648
        try:
            column_header_list_updated = [ re.sub(r'_+','_',x) for x in column_header_list]
            column_header_list_updated = [x[:-1] if x[-1] == '_' else x for x in column_header_list_updated]
        except:
            column_header_list_updated=[]
        return column_header_list_updated

def multilineprocesssing(csv_filename,src_name,input_file_prefix,delimiter_value,multiline_files_list):
    print("in multilineprocesssing")
    try:
        json_obj=multiline_files_list
        inp_f_lst = [StructField('col_'+str(val), StringType(), True) for val in range(json_obj['file_prefix_nm'][input_file_prefix]['total_column'])]

        inp_schema = StructType(inp_f_lst)
    
        csv_input = frmspark.execute_read(infer_schema=False, schema=inp_schema, header=False, quote=None,escape=json_obj['file_prefix_nm'][input_file_prefix]['escape'],csv=csv_filename, delim=delimiter_value,multiLine=True)
        return csv_input
    except Exception as e:
        # xdpLogger("XDPEXP",e)
        tb = traceback.format_exc()
        print(tb)
        xdpLogger("xDP-WAR-041",comment="Application id : {}, Exception Occurred :{}, traceback : {} ".format(application_id,e,tb))                           
        xdpLogger("xDP-WAR-041",comment="Exception Occurred| file level checks failed. | Please check the configuration json in mapping table: {} ".format(app_config.mapping_table))
        entry_list = []
        value_list = [app_config.file_ingestion_detail,str(snap_dt),run_id,str(file_name),src_name,input_file_prefix,'NA','Filelevelcheck','MultiLineProcesssing', 'Fail',utility.timeStamp(),today_1]
        entry_list.append(value_list)
        writeStatus2FileIngestion(entry_list)
        spark.stop()
        sys.exit(1)

def datacleaning(csv_filename,src_name,input_file_prefix,delimiter_value,is_multiline):
    print("in datacleaning")
    try:
        json_obj=json.loads(frmspark.execute_read_ctrl_tbl_api(table = app_config.mapping_table ).filter(F.col('group_name')=='data_clean_configuration').select('val1').rdd.flatMap(lambda x: x).collect()[0])

        if input_file_prefix in json_obj['file_prefix_nm']:
            xdpLogger('xDP-INF-111',comment='File matched in reference_mapping table.')

            inp_f_lst = [StructField('col_'+str(val), StringType(), True) for val in range(json_obj['file_prefix_nm'][input_file_prefix]['total_column'])]
            inp_schema = StructType(inp_f_lst)
            if is_multiline == True:
                csv_input = frmspark.execute_read(infer_schema=False, schema=inp_schema, header=False, quote=None,escape=json_obj['file_prefix_nm'][input_file_prefix]['escape'], csv=csv_filename, delim=delimiter_value,multiLine=True)
                csv_without_bad_data = spark.read.schema(inp_schema).option("escape", json_obj['file_prefix_nm'][input_file_prefix]['escape']).option("delimiter", delimiter_value).option("mode", "DROPMALFORMED").csv(csv_filename)
                print("in multiline read ",csv_input.columns)            

            else:                            
                csv_input = frmspark.execute_read(infer_schema=False, schema=inp_schema, header=False, quote=json_obj['file_prefix_nm'][input_file_prefix]['quote'], escape=json_obj['file_prefix_nm'][input_file_prefix]['escape'], csv=csv_filename, delim=delimiter_value)
                csv_without_bad_data = spark.read.schema(inp_schema).option("quote", json_obj['file_prefix_nm'][input_file_prefix]['quote']).option("escape", json_obj['file_prefix_nm'][input_file_prefix]['escape']).option("delimiter", delimiter_value).option("mode", "DROPMALFORMED").csv(csv_filename)

            if json_obj['file_prefix_nm'][input_file_prefix]['type'] == 'automated':
                bad_records = csv_input.filter(csv_input.col_0.isin(['d','D'])).subtract(csv_without_bad_data.filter(csv_without_bad_data.col_0.isin(['d','D']))).rdd.map(list)
                bad_rec_cnt = bad_records.count()

                if bad_rec_cnt > 0:
                    entry_list = []
                    bad_rows_list = bad_records.take(10)
                    xdpLogger('xDP-INF-111',comment="Rows with extra or less values in File :{}".format(file_name))
                    xdpLogger('xDP-INF-111',comment=bad_rows_list)
                    xdpLogger('xDP-INF-111',comment="Bad Rows with extra or less values in File ,Bad Rows Count:{} ".format(bad_rec_cnt))
                    value_list = [app_config.file_ingestion_detail,str(snap_dt),run_id,str(file_name),src_name,input_file_prefix,'NA','Filelevelcheck','IndividualRowLength', 'Fail',utility.timeStamp(),today_1]
                    entry_list.append(value_list)
                    writeStatus2FileIngestion(entry_list)

            elif json_obj['file_prefix_nm'][input_file_prefix]['type'] == 'manual':
                csv_input_temp = csv_input.withColumn("idx", F.monotonically_increasing_id()).filter(F.col("idx") == 0).drop(F.col("idx"))
                csv_without_bad_data_temp = csv_without_bad_data.withColumn("idx", F.monotonically_increasing_id()).filter(F.col("idx") == 0).drop(F.col("idx"))
                bad_records = csv_input.subtract(csv_input_temp).subtract(csv_without_bad_data.subtract(csv_without_bad_data_temp)).rdd.map(list)
                bad_rec_cnt = bad_records.count()

                if bad_rec_cnt > 0:
                    entry_list = []
                    bad_rows_list = bad_records.take(10)
                    xdpLogger('xDP-INF-111',comment="Rows with extra or less values in File :{}".format(file_name))
                    xdpLogger('xDP-INF-111',comment=bad_rows_list)
                    xdpLogger('xDP-INF-111',comment="Bad Rows with extra or less values in File ,Bad Rows Count:{} ".format(bad_rec_cnt))
                    value_list = [app_config.file_ingestion_detail,str(snap_dt),run_id,str(file_name),src_name,input_file_prefix,'NA','Filelevelcheck','IndividualRowLength', 'Fail',utility.timeStamp(),today_1]
                    entry_list.append(value_list)
                    writeStatus2FileIngestion(entry_list)

            else:
                xdpLogger("xDP-DBG-001",comment="File level check failed | File type should be either manual or automated in {} table".format(app_config.mapping_table))
                entry_list = []
                value_list = [app_config.file_ingestion_detail,str(snap_dt),run_id,str(file_name),src_name,input_file_prefix,'NA','Filelevelcheck','DataCleanProcess', 'Fail',utility.timeStamp(),today_1]
                entry_list.append(value_list)
                writeStatus2FileIngestion(entry_list)
                spark.stop()
                sys.exit(1)

            if len(json_obj['file_prefix_nm'][input_file_prefix]['drop_field_index']) > 0:
                drop_field_list = [str(csv_input[index-1]).replace('Column<','').strip('>') for index in json_obj['file_prefix_nm'][input_file_prefix]['drop_field_index']]
                # Stripping  "b'" as column names are being appended with "b'" prefix in py3
                drop_field_list = [x.strip("b'") for x in drop_field_list ]
                csv_input = csv_input.drop(*drop_field_list)

            csv_input = csv_input.fillna('')

            '''
            hex_list is configured in the data clean configuration that will have the list of hexadecimal values or regular expressions of 
            all the characters that needs to be removed from the file.
            eg,
            '//xa1', wherea1 is the hexadecimal value for inverted exclamation mark, /x denotes hexadecimal format and additional / is used as an escape character. 
            [^//S ] - regular expression to remove non printables (\n \t \v \f ) except empty space   
            '''

            try:
                hex_list = json_obj['file_prefix_nm'][input_file_prefix]['hex_list']
                print("hex_list specified in clean configuration JSON for {}".format(input_file_prefix))
                csv_input = (reduce(lambda memo_df, col_name: memo_df.withColumn(col_name, remove_all_hex(col_name,hex_list)), csv_input.columns, csv_input))  
            except Exception as e:
                xdpLogger("XDPEXP",e)
                print("hex_list not specified in clean configuration JSON for {}".format(input_file_prefix))
                
            return csv_input
    except Exception as e:
       xdpLogger("xDP-WAR-041",comment="Exception Occurred| file level checks failed. | Please check the configuration json in mapping table: {} ".format(app_config.mapping_table))
       entry_list = []
       value_list = [app_config.file_ingestion_detail,str(snap_dt),run_id,str(file_name),src_name,input_file_prefix,'NA','Filelevelcheck','DataCleanProcess', 'Fail',utility.timeStamp(),today_1]
       entry_list.append(value_list)
       writeStatus2FileIngestion(entry_list)
       tb = traceback.format_exc()
       print(tb)
       xdpLogger("xDP-WAR-041",comment="Application id : {}, Exception Occurred :{}, traceback : {} ".format(application_id,e,tb))
       spark.stop()
       sys.exit(1)

def spark_to_pandas_df(df):
    """
    returns a pandas dataframe created from the given spark dataframe 
    """
    try:
        return df.toPandas()
    except Exception:
        xdpLogger('xDP-ERR-112',comment = "failed to convert spark dataframe to pandas dataframe")
        spark.stop()
        sys.exit(1)

def read_file_expctd_list_table():
    return file_expctd_list_pd()

def read_file_checklist_table():
    return file_checklist_pd()

def read_mapping_table():
    return ref_mapping_pd()

def read_file_schema_info_table():
    return file_schema_info_pd()

def preprocessing(src_sys_nm, input_file_prefix):
    """
    PREPROCESSING : THIS FUNCTION WILL CHECK AND SEPERATE THE ADDITIONAL HEADERS FROM THE RAW FILE IF ANY
    THE OUTPUT IS A DATAFRAME WITH SINGLE HEADER FOR ALL TYPE OF FILES
    PRE-FORMATED FILES WITH HTD
    MANUALLY UPLOADED FILES WITH NO HDT COLUMN
    MANUALLY UPLOADED FILES WITH HDT COLUMN
    """
    try:
        get_logger()
        xdpLogger('xDP-INF-001')
        global file_name
        global file_path
        global snap_dt
        global run_id

        file_expctd_df = read_file_expctd_list_table()
        file_checklist_df = read_file_checklist_table()
        file_schema_df = read_file_schema_info_table()
        mapping_df = read_mapping_table()

        file_checklist_schema = StructType([ StructField("file_prefix_nm",StringType(),True), StructField("file_chck_typ",StringType(),True),
                                             StructField("arguments",StringType(),True), StructField("priority",StringType(),True),
                                             StructField("threshold_pre",StringType(),True), StructField("eff_seq",IntegerType(),True),
                                             StructField("status_flag",StringType(),True),  StructField("lastupddttm",StringType(),True)])
        file_checklist_sp = spark.createDataFrame(file_checklist_df, schema = file_checklist_schema)
        
        mapping_schema = StructType([ StructField("group_name",StringType(),True), StructField("key1",StringType(),True),
                                             StructField("key2",StringType(),True), StructField("val1",StringType(),True),
                                             StructField("val2",StringType(),True), StructField("eff_seq",IntegerType(),True),
                                             StructField("status_flag",StringType(),True),  StructField("lastupddttm",StringType(),True)])
        mapping_df = spark.createDataFrame(mapping_df, mapping_schema)
        
        file_expctd_df_filtered = file_expctd_df.loc[file_expctd_df["file_prefix_nm"].str.lower() == input_file_prefix.lower()]
        file_type_result = file_expctd_df_filtered.loc[:, ["file_type","part_file_flag"]].iloc[0]

        is_fw_file = False
        check_fw_result =file_type_result['file_type']
        part_file_result =file_type_result['part_file_flag']
        file_type =str(check_fw_result)

        if str(check_fw_result).lower() == 'fixed_width':
            is_fw_file = True
        part_file_flag = False

        if str(part_file_result).lower() == 'y':
            part_file_flag = True

        xdpLogger('xDP-INF-111',comment="part_file_flag from file_expected_list- {}".format(part_file_flag))

        file_info, meta_data_df,rowcountvariance_df = get_running_file_details(spark, src_sys_nm, input_file_prefix)
        meta_data_df = spark.createDataFrame(meta_data_df)
        rowcountvariance_df = spark.createDataFrame(rowcountvariance_df)

        if src_sys_nm is None or src_sys_nm == '':
            file_base_loc=app_config.file_proc_loc+input_file_prefix+"/"
        else:
            file_base_loc=app_config.file_proc_loc+src_sys_nm+"-"+input_file_prefix+"/"
        file_name=file_info[0].file_nm
        xdpLogger('xDP-INF-111',comment="File Name Fetched,File Name : {}".format(file_name))
        #file_base_loc=app_config.file_proc_loc+input_file_prefix+"/"+file_name
        count=len(file_info)

        delimiter_dataframe = file_expctd_df_filtered.drop_duplicates(['hexacodes_flag','field_delimiter'])[['hexacodes_flag','field_delimiter']]
        delimiter_data = delimiter_dataframe.iloc[0]

        hexacodes_flag = delimiter_data['hexacodes_flag']
        field_delimiter =delimiter_data['field_delimiter']

        field_delimiter = str(field_delimiter.encode().decode('unicode_escape'))
        delimiter_value=codecs.decode(field_delimiter.rjust(2,"0"),"hex") if hexacodes_flag.lower()=='y' else field_delimiter

        for num_rows in range(count):
            try:
                src_name = file_info[num_rows].src_sys_nm
                run_id = file_info[num_rows].run_id
                snap_dt = file_info[num_rows].snap_dt
                file_name = file_info[num_rows].file_nm
                file_path = file_base_loc + file_name

                xdpLogger('xDP-INF-111',comment="File Source Name: {}".format(src_name))
                xdpLogger('xDP-INF-111',comment="File Name: {}. Application ID: {}.".format(file_name, application_id))
                xdpLogger('xDP-INF-111',comment="Fila Path: {}.".format(file_path))

                list_1 = []
                list_2 = []
                df_tail_count = 0
                df_header_count = 0

                csv_filename = file_path
                a = subprocess.call(['aws', 's3', 'ls', csv_filename])
                if a != 0:
                    xdpLogger('xDP-ERR-001',comment="File not present in processing zone. Filename: {}".format(csv_filename))
                    continue

                xdpLogger('xDP-DBG-001',comment="csv file name,csv filename:{}".format(csv_filename))
                colmatch_flag = ''
                bad_rows_cnt = 0

                fetch_col_head_flag = file_expctd_df_filtered
                fetch_col_head_flag.loc[:,'col_header'] = np.select(fetch_col_head_flag['col_header_flag'].isnull(),'Y',fetch_col_head_flag['col_header_flag'])
                col_header_flag = fetch_col_head_flag['col_header'].unique()[0]

                #########################################################################################################################
                ##################### Start:: Jira: WMIT-5079 | Bug fix for data containing quotes and extra column #####################
                #rdd_datafile = sc.textFile(csv_filename).mapPartitions(lambda line: unicode_csv_reader(line,delimiter= delimiter_value, quotechar='"'))
                if is_fw_file == True:
                    initial_df = spark.read.csv(csv_filename)
                    initial_df.createOrReplaceTempView("initial_df_v")
                    is_dataclean = None
                    is_multiline = None
                    delimiter_value = None
                    df_with_identifier = spark.sql("select substring(_c0,1,1) as identifier,substring(_c0,2,length(_c0)) as data from initial_df_v")
                    df_header_only = df_with_identifier.filter((F.col('identifier') == 'H') | (F.col('identifier') == 'h'))
                    header_count = df_header_only.count()
                    tail_count = df_with_identifier.filter((F.col('identifier') == 'T') | (F.col('identifier') == 't')).count()
                    df_only_data = df_with_identifier.filter((F.col('identifier') == 'D') | (F.col('identifier') == 'd'))
                    data_count = df_only_data.count()
                    df_data_with_d_idn = df_with_identifier.filter(~(F.col('identifier')).isin(['t','T','H','h']))
                    df_data_with_d_idn_count = df_data_with_d_idn.count()

                    empty_file_flag = 0
                    no_D_identifer = False

                     
                    rdd_datafile = sc.textFile(csv_filename)
                    if (rdd_datafile.isEmpty() == False):
                            entry_list = []
                            
                            try:
                                file_content_format = file_checklist_sp.filter((F.col('file_prefix_nm')==input_file_prefix ) & (F.col('file_chck_typ')=='fileContentCheck')).select('arguments').rdd.flatMap(lambda x: x).collect()[0]
                            except:
                                xdpLogger('xDP-INF-111',comment="file_content_format not defined in  {}, Processing as Manual file.".format(app_config.file_checklist))
                                file_content_format = ''

                            xdpLogger('xDP-INF-111',comment="col_header_flag - {}".format(col_header_flag))
                            manual_file_flag = False

                            
                            #For files without prefix
                            if  '_no' in file_content_format and file_content_format!='':
                                actual_file_content_format = file_content_format.split('_')[0]
                                xdpLogger('xDP-INF-111',comment="file_content_format - {}".format(file_content_format))
                                xdpLogger('xDP-INF-111',comment="actual_file_content_format - {}".format(actual_file_content_format))
                                file_content_counter = Counter(actual_file_content_format)
                                df_header_count = file_content_counter['H']
                                df_tail_count = file_content_counter['T']
                                xdpLogger('xDP-INF-111',comment="df_header_count - {}".format(df_header_count))
                                xdpLogger('xDP-INF-111',comment="df_tail_count - {}".format(df_tail_count))

                                # handling headers
                                if df_header_count == 0:
                                    data_headers = []
                                    column_header = []
                                elif df_header_count ==1 :
                                    data_headers = rdd_datafile.take(1)
                                    if col_header_flag == 'Y':
                                        column_header = data_headers[0][:]
                                    else:
                                        column_header =[]
                                elif df_header_count ==2 :
                                    data_headers = rdd_datafile.take(2)
                                    column_header = data_headers[1][:]
                                else:
                                    xdpLogger('xDP-ERR-003',comment="Unsupported file_content_format '{file_content_format}' recieved in fileContentCheck - '{file_checklist}' table".format(file_content_format=file_content_format, file_checklist=app_config.file_checklist))
                                    value_list = [app_config.file_ingestion_detail,str(snap_dt),run_id,str(file_name),src_name,input_file_prefix,'NA','Filelevelcheck',"Unsupported file_content_format '{file_content_format}' recieved in fileContentCheck - '{file_checklist}' table".format(file_content_format=file_content_format, file_checklist=app_config.file_checklist), 'Failed',utility.timeStamp(),today_1]
                                    entry_list.append(value_list)
                                    writeStatus2FileIngestion(entry_list)
                                    spark.stop()
                                    sys.exit(1)

                                #excluding all data_headers    
                                rdd_datafile = rdd_datafile.filter(lambda line: line not in data_headers and len(line)>0)
                                df_final_data = spark.createDataFrame(rdd_datafile,StringType())
                                
                                #handling tail
                                if df_tail_count == 0:
                                    last_row = []
                                elif df_tail_count == 1:
                                    total = rdd_datafile.count()
                                    last_row_rdd = rdd_datafile.zipWithIndex().filter(lambda x:  x[1]==total -1)
                                    last_row = last_row_rdd.first()[0][:]
                                    rdd_datafile = rdd_datafile.filter(lambda line: line != last_row and len(line)>0)
                                    df_final_data = spark.createDataFrame(rdd_datafile,StringType())
                                    df_final_data.show()
                                else:
                                    xdpLogger('xDP-ERR-003',comment="Unsupported file_content_format '{file_content_format}' recieved in fileContentCheck - '{file_checklist}' table".format(file_content_format=file_content_format, file_checklist=app_config.file_checklist))
                                    value_list = [app_config.file_ingestion_detail,str(snap_dt),run_id,str(file_name),src_name,input_file_prefix,'NA','Filelevelcheck',"Unsupported file_content_format '{file_content_format}' recieved in fileContentCheck - '{file_checklist}' table".format(file_content_format=file_content_format, file_checklist=app_config.file_checklist), 'Failed',utility.timeStamp(),today_1]
                                    entry_list.append(value_list)
                                    writeStatus2FileIngestion(entry_list)
                                    spark.stop()
                                    sys.exit(1)

                                top10rows=df_final_data.take(10)
                                top10rows = [x for x in top10rows if len(x) > 0 and x is not last_row]
                                top10rows_onlydata=top10rows[:]
                                top10rows_onlydata_with_d_ind=[x for x in top10rows_onlydata if x[0] in ['D']]
                                
                                #no D identifier
                                if len(top10rows_onlydata_with_d_ind)==0 :
                                    column_name_list = []
                                #D identifier present
                                else:
                                    column_name_list = ["H"]
                                    column_header.insert(0,"H")

                                if len(top10rows_onlydata)<=0:
                                    empty_file_flag=1
                                else:
                                    empty_file_flag=0
                                
                                
                                xdpLogger('xDP-INF-111',comment="data_headers  - {}".format(data_headers ))
                                xdpLogger('xDP-INF-111',comment="last_row - {}".format(last_row))

                            else:
                                if(header_count == 1 and col_header_flag!='Y' and data_count==0):
                                    no_D_identifer = True
                                    df_final_data = df_data_with_d_idn
                                    column_name_list = []
                                    column_header=[]
                                    #print("H1D or H1DT type file with no 'D' ind")
                                    if df_data_with_d_idn_count<=0:
                                        empty_file_flag=1

                                elif(header_count == 1 and col_header_flag!='Y' and data_count!=0):
                                    df_final_data = df_data_with_d_idn
                                    column_name_list = ["H"]
                                    column_header=[]
                                    #print("H1D or H1DT type file")
                                    if df_data_with_d_idn_count<=0:
                                        empty_file_flag=1
     
                                elif(header_count==0 and col_header_flag!='Y' and data_count!=0):
                                    df_final_data = df_data_with_d_idn
                                    column_name_list = []
                                    column_header=[]
                                    #print("D or DT type file with 'D' ind")
                                    if df_data_with_d_idn_count<=0:
                                        empty_file_flag=1

                                elif(header_count == 0 and col_header_flag!='Y' and data_count==0):
                                    no_D_identifer = True
                                    df_final_data = df_data_with_d_idn
                                    df_final_data = df_data_with_d_idn
                                    column_name_list = []
                                    column_header=[]
                                    #print("D or DT type file with no 'D' ind")
                                    if df_data_with_d_idn_count<=0:
                                        empty_file_flag=1


                                elif (header_count == 1 and col_header_flag=='Y'):
                                    df_final_data = df_data_with_d_idn
                                    column_name_list = ["H"]
                                    column_header_str = str(df_header_only.select(F.col('data')).collect()[0][0])
                                    column_header = column_list_fw(column_header_str,input_file_prefix)
                                    #print("H2D or H2DT type file")
                                    if df_data_with_d_idn_count<=0:
                                        empty_file_flag=1

                                elif header_count > 1:
                                    df_final_data = df_data_with_d_idn
                                    column_name_list = ["H"]
                                    column_header_str = str(df_header_only.select(F.col('data')).collect()[1][0])
                                    column_header = column_list_fw(column_header_str,input_file_prefix)
                                    #print("H1H2DT or H1H2D type file")
                                    if df_data_with_d_idn_count<=0:
                                        empty_file_flag=1

                    
                    else:
                        empty_file_flag=1

                    if empty_file_flag==1:
                        empty_file_list=[]
                        list_1=[app_config.file_ingestion_detail,snap_dt,run_id,file_name,src_name,input_file_prefix,'NA','Filelevelcheck', 'Emptyfilecheck', 'Fail',utility.timeStamp(),today_1]
                        empty_file_list.append(list_1)
                        utility.moveFile(file_name, file_base_loc, app_config.empty_file_path)
                        utility.delUnzipFile(file_name, file_base_loc)
                        list_2=[app_config.file_ingestion_detail,snap_dt,run_id,file_name,src_name,input_file_prefix,'NA','FileTransfer', 'EmptyfileTransfer', 'Pass',utility.timeStamp(),today_1]
                        empty_file_list.append(list_2)
                        writeStatus2FileIngestion(empty_file_list)
                        continue

                    # data file headers cleaning
                    mapping_values_rdd  = mapping_details("tabcol_repmapping")
                    column_header_list = column_name_sub(column_header,mapping_values_rdd)
                    #file Schema info columns based on file prefix name
                    col_list = column_list(input_file_prefix,column_name_list)
                    final_column_count = len(col_list)

                    xdpLogger("xDP-DBG-001",comment="Trying to match column list from file schema info & file header")
                    if col_list == column_header_list:
                            colmatch_flag = 'equal'

                    elif((header_count == 1 and col_header_flag!='Y') or (header_count==0 and col_header_flag!='Y')or (header_count==0 and col_header_flag=='Y')):
                            colmatch_flag = 'equal'

                    else:
                        diff = list(set(column_header_list) - set(col_list))
                        excluding_extra = [col for col in column_header_list if col not in diff]
                        if excluding_extra == col_list:
                            colmatch_flag = 'extra'
                        else:
                            entry_list = []
                            value_list = [app_config.file_ingestion_detail,str(snap_dt),run_id,str(file_name),src_name,input_file_prefix,'NA','Filelevelcheck','Schemavalidation', 'Fail',utility.timeStamp(),today_1]
                            utility.control_reporting_tbl(str(snap_dt),run_id,input_file_prefix,src_name,str(file_name),"File","NA","Schemavalidation","Fail","NA","NA","NA","NA","NA","NA","Schema Matching failed",utility.dateToday(),utility.timeStamp())
                            entry_list.append(value_list)
                            writeStatus2FileIngestion(entry_list)
                            xdpLogger("xDP-WAR-054",comment="File Schema:{} not matching with entries in file_schema_info table:{}".format(column_header_list,col_list))

                            if utility.moveFile(file_name, file_base_loc, app_config.rejected_file_path):
                                list_schema_fail= (app_config.file_ingestion_detail,snap_dt,run_id,file_name,src_name,input_file_prefix,'NA','FileTransfer', 'Schemavalidation', 'Pass',utility.timeStamp(),today_1)
                                entry_list.clear()
                                entry_list.append(list_schema_fail)
                                writeStatus2FileIngestion(entry_list)
                                utility.delUnzipFile(file_name, file_base_loc)
                                xdpLogger('xDP-WAR-041',comment="File Movement Check Status Info written to FILE_INGESTION_DETAIL Table. File name: {} moved to rejected folder".format(file_name))
                            else:
                                list_schema_fail= (app_config.file_ingestion_detail,snap_dt,run_id,file_name,src_name,input_file_prefix,'NA','FileTransfer', 'Schemavalidation', 'Fail',utility.timeStamp(),today_1)
                                entry_list.append(list_schema_fail)
                                writeStatus2FileIngestion(entry_list)
                                utility.delUnzipFile(file_name, file_base_loc)
                                xdpLogger("xDP-DBG-001",comment="*****************File Movement Status Info written to FILE_INGESTION_DETAIL Table*********************")
                            sys.exit(1)

                    xdpLogger("xDP-DBG-001",comment="File Schema matched with file schema info table")
                    if no_D_identifer == True:
                        df_final_data = df_final_data.select(concat(F.col("identifier"), lit(""), F.col("data")))
                    else:
                        df_final_data = df_final_data.drop("identifier")
                    data_rdd = df_final_data.rdd.map(list)

                    file_schema_filtered_df = file_schema_df.loc[file_schema_df["file_prefix_nm"].str.lower() == input_file_prefix.lower()]
                    file_schema_filtered_df['col_length'] = file_schema_filtered_df['col_length'].astype(int)
                    total_col_length = file_schema_filtered_df['col_length'].sum() 

                    bad_data_rdd = data_rdd.filter(lambda x: len(x[0]) < total_col_length)
                    bad_rows_cnt = bad_data_rdd.count()
                    filter_data_rdd = data_rdd.filter(lambda x: len(x[0]) >= total_col_length)
                    good_data_rdd = filter_data_rdd.map(lambda x: x[0][0:total_col_length])
                    final_df = good_data_rdd.map(lambda x: (x,)).toDF()
                    file_content_dict = {}
                    file_content_dict['H'] = header_count
                    file_content_dict['T'] = tail_count
                    file_content_dict['D'] = 1
                    initial_list = []
                    col_list = column_list(input_file_prefix,initial_list)
                    bad_rows_rdd = bad_data_rdd.take(10)
                    if bad_rows_cnt > 0:
                        entry_list = []
                        bad_rows_list = bad_rows_rdd
                        xdpLogger('xDP-INF-111',comment="Rows with extra or less values in File :{}".format(file_name))
                        xdpLogger('xDP-INF-111',comment=str(bad_rows_list))
                        xdpLogger('xDP-INF-111',comment="Bad Rows with extra or less values in File ,Bad Rows Count:{} ".format(bad_rows_cnt))
                        value_list = [app_config.file_ingestion_detail,str(snap_dt),run_id,str(file_name),src_name,input_file_prefix,'NA','Filelevelcheck','IndividualRowLength', 'Fail',utility.timeStamp(),today_1]
                        entry_list.append(value_list)
                        writeStatus2FileIngestion(entry_list)
                    xdpLogger("xDP-DBG-001",comment="Calling file level checks")
                    file_level_check(file_expctd_df, file_checklist_df, file_schema_df, mapping_df ,is_fw_file,is_multiline,is_dataclean,csv_filename,delimiter_value,input_file_prefix,snap_dt,run_id,src_name,file_name,meta_data_df,rowcountvariance_df, final_df, bad_rows_cnt, header_count, last_row = '', mapping_list='', col_list=col_list, data_headers='', top10rows = '',data_count= data_count,file_content_dict = file_content_dict,part_file_flag = part_file_flag)
                else:
                    try:
                        json_obj=json.loads(mapping_df.filter(F.col('group_name')=='data_clean_configuration').select('val1').rdd.flatMap(lambda x: x).collect()[0])
                        multiline_files_list = json.loads(mapping_df.filter(F.col('group_name')=='listOfMultilineFiles').select('val1').rdd.flatMap(lambda x: x).collect()[0])
                        xml_obj=json.loads(mapping_df.filter(F.col('group_name')=='xml_configuration').select('val1').rdd.flatMap(lambda x: x).collect()[0])
                        xml_config = False
                        is_multiline = False
                        is_dataclean = False
                        quote_flag = None

                        if input_file_prefix in multiline_files_list['file_prefix_nm']:
                            is_multiline = True
                        if input_file_prefix in json_obj['file_prefix_nm']:
                            is_dataclean = True
                        if input_file_prefix in xml_obj['file_prefix_nm']:
                            xml_config=True
                        if is_multiline == True:
                            # if is_dataclean == True:
                            xdpLogger('xDP-INF-111',comment='multiline == True')
                            xdpLogger('xDP-INF-111',comment='File matched in reference_mapping table.')
                            csv_input = multilineprocesssing(csv_filename,src_name,input_file_prefix,delimiter_value,multiline_files_list)
                            if input_file_prefix in json_obj['file_prefix_nm']:
                                try:
                                    quote_flag_value=json_obj['file_prefix_nm'][input_file_prefix]['quote_flag']
                                except:
                                    quote_flag_value="N"
                            else:
                                quote_flag_value="N"
                            if quote_flag_value =="Y":
                                rdd_datafile = csv_input.rdd.map(list).map(lambda line: [ '' if item is None else item.replace(u'\ufffd','"') for item in line])
                            else:
                                rdd_datafile = csv_input.rdd.map(list).map(lambda line: [ '' if item is None else item.replace(u'\ufffd','"').strip('"').strip("'") for item in line])
                        elif is_dataclean == True and is_multiline == False:
                            xdpLogger('xDP-INF-111',comment='dataclean == True and multiline == False')
                            xdpLogger('xDP-INF-111',comment='File matched in reference_mapping table.')
                            csv_input = datacleaning(csv_filename,src_name,input_file_prefix,delimiter_value,is_multiline)
                            if input_file_prefix in json_obj['file_prefix_nm']:
                                 try:
                                    quote_flag_value=json_obj['file_prefix_nm'][input_file_prefix]['quote_flag']
                                 except:
                                    quote_flag_value='N'
                            else:
                                quote_flag_value="N"
                            if quote_flag_value =="Y":
                                rdd_datafile = csv_input.rdd.map(list).map(lambda line: [ '' if item is None else item.replace(u'\ufffd','"') for item in line])
                            else:
                                rdd_datafile = csv_input.rdd.map(list).map(lambda line: [ '' if item is None else item.replace(u'\ufffd','"').strip('"').strip("'") for item in line])
                        elif xml_config == True:
                            xdpLogger('xDP-INF-111',comment='File matched in reference_mapping table.')
                            csv_input = utility.xmlprocessing(csv_filename,src_name,input_file_prefix,delimiter_value,xml_obj,snap_dt)
                            print(csv_input.show(3))
                            df_tmp_cols = csv_input.columns
                            rdd_cols = spark.sparkContext.parallelize([df_tmp_cols])
                            print("RDD Cols",rdd_cols)
                            rdd_datafile_raw = csv_input.rdd.map(list)
                            rdd_datafile = rdd_cols.union(rdd_datafile_raw)
                        else:
                            xdpLogger('xDP-INF-111',comment='File not matched in reference_mapping table | Processing as usual.')
                            if '.gz' in csv_filename:
                                print("inside gz")
                                print(csv_filename)
                                bucket, gzipped_key = path_to_bucket_key(csv_filename)
                                uncompressed_key = gzipped_key[:-3]
                                s3 = boto3.client('s3')  
                                s3.upload_fileobj(                    
                                    Fileobj=gzip.GzipFile(             
                                        None,                           
                                        'rb',                          
                                        fileobj=BytesIO(s3.get_object(Bucket=bucket, Key=gzipped_key)['Body'].read())),
                                    Bucket=bucket,
                                    Key=uncompressed_key) 
                                csv_filename_unzipped = csv_filename[:-3]
                                rdd_datafile = sc.textFile(csv_filename_unzipped).mapPartitions(lambda line: unicode_csv_reader(line,delimiter= delimiter_value,quotechar='"'))
                            else:
                                rdd_datafile = sc.textFile(csv_filename).mapPartitions(lambda line: unicode_csv_reader(line,delimiter= delimiter_value,quotechar='"'))

                    except Exception as e:
                       xdpLogger("xDP-WAR-041",comment="Exception Occurred| file level checks failed. | Please check the configuration json in mapping table: {} ".format(app_config.mapping_table))
                       entry_list = []
                       value_list = [app_config.file_ingestion_detail,str(snap_dt),run_id,str(file_name),src_name,input_file_prefix,'NA','Filelevelcheck','DataCleanProcess', 'Fail',utility.timeStamp(),today_1]
                       entry_list.append(value_list)
                       writeStatus2FileIngestion(entry_list)
                       tb = traceback.format_exc()
                       xdpLogger("xDP-WAR-041",comment="Application id : {}, Exception Occurred :{}, traceback : {} ".format(application_id,e,tb))
                       spark.stop()
                       sys.exit(1)
                    ####################### End:: Jira: WMIT-5079 | Bug fix for data containing quotes and extra column #####################
                    #########################################################################################################################
                    if (rdd_datafile.isEmpty() == False):
                            # pick file content check - argument
                            entry_list = []
                            
                            try:
                                file_content_format = file_checklist_sp.filter((F.col('file_prefix_nm')==input_file_prefix ) & (F.col('file_chck_typ')=='fileContentCheck')).select('arguments').rdd.flatMap(lambda x: x).collect()[0]
                            except:
                                xdpLogger('xDP-INF-111',comment="file_content_format not defined in  {}, Processing as Manual file.".format(app_config.file_checklist))
                                file_content_format = ''

                            xdpLogger('xDP-INF-111',comment="col_header_flag - {}".format(col_header_flag))
                            manual_file_flag = False

                            #Processing files without prefix
                            if  '_no' in file_content_format and file_content_format!='':
                                actual_file_content_format = file_content_format.split('_')[0]
                                xdpLogger('xDP-INF-111',comment="file_content_format - {}".format(file_content_format))
                                xdpLogger('xDP-INF-111',comment="actual_file_content_format - {}".format(actual_file_content_format))
                                file_content_counter = Counter(actual_file_content_format)
                                df_header_count = file_content_counter['H']
                                df_tail_count = file_content_counter['T']
                                xdpLogger('xDP-INF-111',comment="df_header_count - {}".format(df_header_count))
                                xdpLogger('xDP-INF-111',comment="df_tail_count - {}".format(df_tail_count))


                                # handling headers
                                if df_header_count == 0:
                                    data_headers = []
                                    column_header = []
                                elif xml_config == True:
                                    data_headers = rdd_datafile.take(1)
                                    df_tail_count = 0
                                    if col_header_flag == 'Y':
                                        column_header = data_headers[0][:]
                                        #print("..................................................",column_header)
                                    else:
                                        column_header =[]
                                        print("################No Header#####")
                                elif df_header_count ==1 :
                                    data_headers = rdd_datafile.take(1)
                                    if col_header_flag == 'Y':
                                        column_header = data_headers[0][:]
                                    else:
                                        column_header =[]
                                elif df_header_count ==2 :
                                    data_headers = rdd_datafile.take(2)
                                    column_header = data_headers[1][:]
                                else:
                                    xdpLogger('xDP-ERR-003',comment="Unsupported file_content_format '{file_content_format}' recieved in fileContentCheck - '{file_checklist}' table".format(file_content_format=file_content_format, file_checklist=app_config.file_checklist))
                                    value_list = [app_config.file_ingestion_detail,str(snap_dt),run_id,str(file_name),src_name,input_file_prefix,'NA','Filelevelcheck',"Unsupported file_content_format '{file_content_format}' recieved in fileContentCheck - '{file_checklist}' table".format(file_content_format=file_content_format, file_checklist=app_config.file_checklist), 'Failed',utility.timeStamp(),today_1]
                                    entry_list.append(value_list)
                                    writeStatus2FileIngestion(entry_list)
                                    spark.stop()
                                    sys.exit(1)

                                #excluding all data_headers    
                                rdd_datafile = rdd_datafile.filter(lambda line: line not in data_headers and len(line)>0)
                                #handling tail
                                if df_tail_count == 0:
                                    last_row = []
                                elif df_tail_count == 1:
                                    total = rdd_datafile.count()
                                    last_row_rdd = rdd_datafile.zipWithIndex().filter(lambda x:  x[1]==total -1)
                                    last_row = last_row_rdd.first()[0][:]
                                    rdd_datafile = rdd_datafile.filter(lambda line: line != last_row and len(line)>0)
                                else:
                                    xdpLogger('xDP-ERR-003',comment="Unsupported file_content_format '{file_content_format}' recieved in fileContentCheck - '{file_checklist}' table".format(file_content_format=file_content_format, file_checklist=app_config.file_checklist))
                                    value_list = [app_config.file_ingestion_detail,str(snap_dt),run_id,str(file_name),src_name,input_file_prefix,'NA','Filelevelcheck',"Unsupported file_content_format '{file_content_format}' recieved in fileContentCheck - '{file_checklist}' table".format(file_content_format=file_content_format, file_checklist=app_config.file_checklist), 'Failed',utility.timeStamp(),today_1]
                                    entry_list.append(value_list)
                                    writeStatus2FileIngestion(entry_list)
                                    spark.stop()
                                    sys.exit(1)

                                top10rows=rdd_datafile.take(10)
                                top10rows = [x for x in top10rows if len(x) > 0 and x is not last_row]
                                top10rows_onlydata=top10rows[:]
                                top10rows_onlydata_with_d_ind=[x for x in top10rows_onlydata if x[0] in ['D']]
                                
                                #no D identifier
                                if len(top10rows_onlydata_with_d_ind)==0 :
                                    column_name_list = []
                                #D identifier present
                                else:
                                    column_name_list = ["H"]
                                    column_header.insert(0,"H")

                                if len(top10rows_onlydata)<=0:
                                    empty_file_flag=1
                                else:
                                    empty_file_flag=0
                                
                                
                                xdpLogger('xDP-INF-111',comment="data_headers  - {}".format(data_headers ))
                                xdpLogger('xDP-INF-111',comment="last_row - {}".format(last_row))
                                                            
                            #Processing files with H T identifier
                            else:
    
                                top10rows=rdd_datafile.take(10)
                                top10rows = [x for x in top10rows if len(x) > 0]
                                top10rows = [x for x in top10rows if x[0] not in ['T','t']]
                                #print(top10rows)
                                top10rows_onlydata=[x for x in top10rows if len(x) > 0 and x[0] not in ['h','H','T','t']]
                                top10rows_onlydata_with_d_ind=[x for x in top10rows_onlydata if x[0] in ['D']]
                                xdpLogger('xDP-INF-111',comment="Length of data rows in top 10 rows {}".format(top10rows_onlydata))
                                empty_file_flag=0


                                data_headers=[x for x in top10rows if len(x) > 0 and x[0] in ['h','H']]
                                df_header_count=len(data_headers)
                                xdpLogger('xDP-INF-111',comment="Header count for file {}:{}".format(file_name, df_header_count))

                                if(df_header_count == 1 and col_header_flag!='Y' and len(top10rows_onlydata_with_d_ind)==0):
                                        last_row = rdd_datafile.filter(lambda x: x[0] in ['t','T'])
                                        rdd_datafile = rdd_datafile.filter(lambda line: line[0] not in  ['H','h','t','T'])
                                        column_name_list = []
                                        column_header=[]
                                        #print("H1D or H1DT type file with no 'D' ind")
                                        if len(top10rows_onlydata)<=0:
                                                empty_file_flag=1

                                elif(df_header_count == 1 and col_header_flag!='Y' and len(top10rows_onlydata_with_d_ind)!=0):
                                        last_row = rdd_datafile.filter(lambda x: x[0] in ['t','T'])
                                        rdd_datafile = rdd_datafile.filter(lambda line: line[0] not in  ['H','h','t','T'])
                                        column_name_list = ["H"]
                                        column_header=[]
                                        #print("H1D or H1DT type file")
                                        if len(top10rows_onlydata)<=0:
                                                empty_file_flag=1

                                elif(df_header_count==0 and col_header_flag!='Y'):
                                        last_row = rdd_datafile.filter(lambda x: x[0] in ['t','T'])
                                        rdd_datafile = rdd_datafile.filter(lambda line: line[0] not in  ['t','T'])
                                        column_name_list = ["H"]
                                        column_header=[]
                                        #print("DT or D type file")
                                        if len(top10rows_onlydata)<=0:
                                                empty_file_flag=1


                                elif (df_header_count == 1 and col_header_flag=='Y'):
                                        last_row = rdd_datafile.filter(lambda x: x[0] in ['t','T'])
                                        rdd_datafile = rdd_datafile.filter(lambda line: line[0] not in  ['H','h','t','T'])
                                        column_name_list = ["H"]
                                        column_header = data_headers[0]
                                        #print("H2D or H2DT type file")
                                        if len(top10rows_onlydata)<=0:
                                                empty_file_flag=1
                                elif df_header_count > 1:
                                        last_row = rdd_datafile.filter(lambda x: x[0] in ['t','T'])
                                        rdd_datafile = rdd_datafile.filter(lambda line: line[0] not in  ['H','h','t','T'])
                                        column_name_list = ["H"]
                                        column_header = data_headers[1]
                                        #print("H1H2DT or H1H2D type file")
                                        if len(top10rows_onlydata)<=0:
                                                empty_file_flag=1


                                else:
                                        last_row=[]
                                        column_name_list = []
                                        column_header = top10rows[0]
                                        manual_file_flag = True
                                #have to test the above syntax to consider as top row when there is no HHDT or HDT in file
                                        rdd_datafile = rdd_datafile.filter(lambda line: line !=column_header)
                        #print("D type file with col header")
                                        if len(top10rows_onlydata)-1<=0:
                                                empty_file_flag=1

                    else:
                        empty_file_flag=1

                    if empty_file_flag==1:
                        empty_file_list=[]
                        list_1=[app_config.file_ingestion_detail,snap_dt,run_id,file_name,src_name,input_file_prefix,'NA','Filelevelcheck', 'Emptyfilecheck', 'Fail',utility.timeStamp(),today_1]
                        empty_file_list.append(list_1)
                        utility.moveFile(file_name, file_base_loc, app_config.empty_file_path)
                        list_2=[app_config.file_ingestion_detail,snap_dt,run_id,file_name,src_name,input_file_prefix,'NA','FileTransfer', 'EmptyfileTransfer', 'Pass',utility.timeStamp(),today_1]
                        empty_file_list.append(list_2)
                        writeStatus2FileIngestion(empty_file_list)
                        utility.delUnzipFile(file_name, file_base_loc)
                        continue


                    # data file headers cleaning
                    mapping_values_rdd  = mapping_details("tabcol_repmapping")
                    column_header_list = column_name_sub(column_header,mapping_values_rdd)
                    #file Schema info columns based on file prefix name
                    col_list = column_list(input_file_prefix,column_name_list)
                    final_column_count = len(col_list)

                    xdpLogger("xDP-DBG-001",comment="Trying to match column list from file schema info & file header")
                    if col_list == column_header_list:
                            colmatch_flag = 'equal'

                    elif((df_header_count == 1 and col_header_flag!='Y') or (df_header_count==0 and col_header_flag!='Y')):
                            colmatch_flag = 'equal'
                    # Bug fix for CDL-1804
                    elif((xml_config == True and col_header_flag=='Y')):
                            colmatch_flag = 'equal'
                    else:
                        diff = list(set(column_header_list) - set(col_list))
                        excluding_extra = [col for col in column_header_list if col not in diff]
                        if excluding_extra == col_list:
                            colmatch_flag = 'extra'
                        else:
                            entry_list = []
                            value_list = [app_config.file_ingestion_detail,str(snap_dt),run_id,str(file_name),src_name,input_file_prefix,'NA','Filelevelcheck','Schemavalidation', 'Fail',utility.timeStamp(),today_1]
                            utility.control_reporting_tbl(str(snap_dt),run_id,input_file_prefix,src_name,str(file_name),"File","NA","Schemavalidation","Fail","NA","NA","NA","NA","NA","NA","Schema Matching failed",utility.dateToday(),utility.timeStamp())
                            entry_list.append(value_list)
                            writeStatus2FileIngestion(entry_list)
                            xdpLogger("xDP-WAR-054",comment="File Schema:{} not matching with entries in file_schema_info table:{}".format(column_header_list,col_list))

                            if utility.moveFile(file_name, file_base_loc, app_config.rejected_file_path):
                                list_schema_fail= (app_config.file_ingestion_detail,snap_dt,run_id,file_name,src_name,input_file_prefix,'NA','FileTransfer', 'Schemavalidation', 'Pass',utility.timeStamp(),today_1)
                                entry_list.clear()
                                entry_list.append(list_schema_fail)
                                writeStatus2FileIngestion(entry_list)
                                utility.delUnzipFile(file_name, file_base_loc)
                                xdpLogger('xDP-WAR-041',comment="File Movement Check Status Info written to FILE_INGESTION_DETAIL Table. File name: {} moved to rejected folder".format(file_name))
                            else:
                                list_schema_fail= (app_config.file_ingestion_detail,snap_dt,run_id,file_name,src_name,input_file_prefix,'NA','FileTransfer', 'Schemavalidation', 'Fail',utility.timeStamp(),today_1)
                                entry_list.append(list_schema_fail)
                                writeStatus2FileIngestion(entry_list)
                                utility.delUnzipFile(file_name, file_base_loc)
                                xdpLogger("xDP-DBG-001",comment="*****************File Movement Status Info written to FILE_INGESTION_DETAIL Table*********************")
                            sys.exit(1)
                    bucket_name = csv_filename[5:].split('/')[0]
                    key = '/'.join(csv_filename[5:].split('/')[1:])
                    session = botocore.session.get_session()
                    client = session.create_client('s3')
                    obj_dict = client.list_objects(Bucket=bucket_name,Prefix=key)
                    file_size = obj_dict['Contents'][0]['Size']
                    if '.gz' in csv_filename:
                        num_partitions = int(file_size/(1024*1024*20))+1
                    else:
                        num_partitions = int(file_size/(1024*1024*200))+1
                    rdd_datafile = rdd_datafile.repartition(num_partitions)

                    xdpLogger("xDP-DBG-001",comment="File Schema matched with file schema info table")
                    bad_rows = rdd_datafile.filter(lambda x: len(x) < final_column_count)
                    filtered_rows = rdd_datafile.filter(lambda x: len(x) >= final_column_count)
                    good_rows = filtered_rows.map(lambda x: x[0:final_column_count])

                    xdpLogger("xDP-DBG-001",comment="Getting bad data count")
                    bad_rows_rdd = bad_rows.take(10)
                    bad_rows_cnt=bad_rows.count()
                    if bad_rows_cnt > 0:
                        entry_list = []
                        bad_rows_list = bad_rows_rdd
                        xdpLogger('xDP-INF-111',comment="Rows with extra or less values in File :{}".format(file_name))
                        xdpLogger('xDP-INF-111',comment=str(bad_rows_list))
                        xdpLogger('xDP-INF-111',comment="Bad Rows with extra or less values in File ,Bad Rows Count:{} ".format(bad_rows_cnt))
                        value_list = [app_config.file_ingestion_detail,str(snap_dt),run_id,str(file_name),src_name,input_file_prefix,'NA','Filelevelcheck','IndividualRowLength', 'Fail',utility.timeStamp(),today_1]
                        entry_list.append(value_list)
                        writeStatus2FileIngestion(entry_list)

                    xdpLogger("xDP-DBG-001",comment="Converting Good data to Dataframe")
                    df = good_rows.toDF(column_name_list)
                    xdpLogger('xDP-INF-111',comment="Good rows count in File ,Count:{} ".format(df.count()))

                    df.persist(StorageLevel.MEMORY_AND_DISK)
                    
                    xdpLogger("xDP-DBG-001",comment="Calling file level checks")
                    file_level_check(file_expctd_df, file_checklist_df, file_schema_df, mapping_df, is_fw_file,is_multiline,is_dataclean,csv_filename,delimiter_value,input_file_prefix,snap_dt,run_id,src_name,file_name,meta_data_df,rowcountvariance_df, df, bad_rows_cnt, df_header_count, last_row = last_row, mapping_list=mapping_values_rdd, col_list=col_list, data_headers=data_headers, top10rows = top10rows,manual_file_flag = manual_file_flag,part_file_flag = part_file_flag,file_content_format=file_content_format,df_tail_count=df_tail_count)
                    if '.gz' in csv_filename:
                        c = subprocess.call(['aws','s3','rm',csv_filename_unzipped])

            except frmexceptions.InvalidS3PathException as exp:
                xdpLogger('xDP-WAR-031',comment="Required file is not available in Processing Zone")
                continue

    except Exception as e:
        #xdpLogger("XDPEXP",e)
        tb = traceback.format_exc()
        print(tb)
        xdpLogger("xDP-WAR-041",comment="Exception Occurred, file level checks failed")
        xdpLogger("xDP-WAR-041",comment="Application id : {}, Exception Occurred :{}, traceback : {} ".format(application_id,e,tb))
        try:
            if '.gz' in csv_filename:
                c = subprocess.call(['aws', 's3', 'rm', csv_filename_unzipped])
        except:
            pass
        sys.exit(1)

def logEntry(check,check_f,src_sys_nm,input_file_prefix,success_list,failure_list):
    """
    LOGENTRY FUNCTION ADDS THE STATUS TO ACTION TO SUCCESS AND FAILURE LIST AND RETURNS BACK TO BE WRITTEN TO TABLE
    """
    try:
        get_logger()
        xdpLogger('xDP-INF-001')
        print(check,check_f)
        #if check:
            #xdpLogger("xDP-DBG-001",comment="***************************************Number of rows match***************************************")
            #list_1= (app_config.file_ingestion_detail,snap_dt,run_id,file_name,src_sys_nm,input_file_prefix,'NA','Filelevelcheck',check, 'Pass',utility.timeStamp(),today_1)
            #success_list.append(list_1)
        #U.writeStatusInfo(file_ingestion_detail, i, file_prefix,'NA', 'File_check', 'rowCountCheck', 'Pass', timestamp)
            #xdpLogger("xDP-DBG-001",comment="******************DQ Check Status Info written to FILE_INGESTION_DETAIL Table*********************")
        if check_f == 'stop':
            list_1= (app_config.file_ingestion_detail,snap_dt,run_id,file_name,src_sys_nm,input_file_prefix,'NA','Filelevelcheck',check, 'Waiting',utility.timeStamp(),today_1)
            utility.delUnzipFile(file_name, file_base_loc)
            success_list.append(list_1)
        elif check:
            xdpLogger("xDP-DBG-001",comment="***************************************Number of rows match***************************************")
            list_1= (app_config.file_ingestion_detail,snap_dt,run_id,file_name,src_sys_nm,input_file_prefix,'NA','Filelevelcheck',check, 'Pass',utility.timeStamp(),today_1)
            success_list.append(list_1)
        #U.writeStatusInfo(file_ingestion_detail, i, file_prefix,'NA', 'File_check', 'rowCountCheck', 'Pass', timestamp)
            xdpLogger("xDP-DBG-001",comment="******************DQ Check Status Info written to FILE_INGESTION_DETAIL Table*********************")
        elif check == 'Skipped':
            list_1= (app_config.file_ingestion_detail,snap_dt,run_id,file_name,src_sys_nm,input_file_prefix,'NA','Filelevelcheck',check_f, 'No requieddetails',utility.timeStamp(),today_1)
            utility.delUnzipFile(file_name, file_base_loc)
            success_list.append(list_1)
        else:
            xdpLogger("xDP-DBG-001",comment="************************************Number of rows do not match***********************************")
            list_2= (app_config.file_ingestion_detail,snap_dt,run_id,file_name,src_sys_nm,input_file_prefix,'NA','Filelevelcheck',check_f, 'Fail',utility.timeStamp(),today_1)
            failure_list.append(list_2)
            #U.writeStatusInfo(file_ingestion_detail, i, file_prefix,'NA', 'File_check', 'rowCountCheck', 'Fail', timestamp)
            xdpLogger("xDP-DBG-001",comment="******************DQ Check Status Info written to FILE_INGESTION_DETAIL Table*********************")

            if utility.moveFile(file_name, file_base_loc, app_config.rejected_file_path):
                list_2= (app_config.file_ingestion_detail,snap_dt,run_id,file_name,src_sys_nm,input_file_prefix,'NA','FileTransfer', check_f, 'Pass',utility.timeStamp(),today_1)
                failure_list.append(list_2)
                utility.delUnzipFile(file_name, file_base_loc)
                #U.writeStatusInfo(file_ingestion_detail, i, file_prefix,'NA','File_movement', 'Error_File_Transfer', 'Pass', timestamp)
                xdpLogger('xDP-WAR-041',comment="File Movement Check Status Info written to FILE_INGESTION_DETAIL Table. File name: {} moved to rejected folder".format(file_name))
            else:
                list_2= (app_config.file_ingestion_detail,snap_dt,run_id,file_name,src_sys_nm,input_file_prefix,'NA','FileTransfer', check_f, 'Fail',utility.timeStamp(),today_1)
                failure_list.append(list_2)
                utility.delUnzipFile(file_name, file_base_loc)
                #U.writeStatusInfo(file_ingestion_detail, i, file_prefix, 'NA','File_movement', 'Error_File_Transfer', 'Fail', timestamp)
                xdpLogger("xDP-DBG-001",comment="*****************File Movement Status Info written to FILE_INGESTION_DETAIL Table*********************")
        return success_list,failure_list

    except Exception as e:
    #  xdpLogger("XDPEXP",e)
       tb = traceback.format_exc()
       xdpLogger("xDP-WAR-041",comment="Application id : {}, Exception Occurred :{}, traceback : {} ".format(application_id,e,tb))

def read_config_table(process_latest):
    try:
        get_logger()
        xdpLogger('xDP-INF-001')
        read_config_sql="select * from " + app_config.file_checklist
        read_config = frmspark.execute_query_ctrl_tbl_api(read_config_sql) #Changes for Jira::WMIT-5794 | to reduce number of partitions in dataframe
        read_config.createOrReplaceTempView("config_file_checks")
        process_latest.createOrReplaceTempView("process_latest")
        files_check_sql="""select file.src_sys_nm,
                                file.file_prefix_nm,
                                file.file_nm,
                                config.file_chck_typ,
                                file.snap_dt,
                                file.run_id
                          from process_latest file left outer join config_file_checks config
                          on file.file_prefix_nm = config.file_prefix_nm
                                 """
        files_check = frmspark.execute_query_ctrl_tbl_api(files_check_sql) #Changes for Jira::WMIT-5794 | to reduce number of partitions in dataframe
        #files_check.show()
        return files_check
    except Exception as e:
    #  xdpLogger("XDPEXP",e)
       tb = traceback.format_exc()
       xdpLogger("xDP-WAR-041",comment="Application id : {}, Exception Occurred :{}, traceback : {} ".format(application_id,e,tb))


def writeStatus2FileIngestion(input_list):
    try:
    #file_ingestion_detail="wcr_ctrl_db.file_ingestion_detail"
        get_logger()
        xdpLogger('xDP-INF-001')
        xdpLogger('xDP-INF-111',comment="Input list for write operation,InputList:{}".format(input_list))
        try:
            if input_list:
        #print(input_list)
                result_df = sc.parallelize(input_list).toDF(["Targetdatabase","snap_dt" ,"run_id","file_nm","src_sys_nm","file_prefix_nm","glue_table_nm" ,"action_type","action_typ_details","status","time","run_dt"]).select("snap_dt" ,"run_id","file_nm","src_sys_nm","file_prefix_nm","glue_table_nm" ,"action_type","action_typ_details","status","time","run_dt")
                #result_df.write.insertInto(app_config.file_ingestion_detail, overwrite=False)
        #result_df.show()
                frmspark.write_ctrl_tbl_api(source_dataframe=result_df, target_table=app_config.file_ingestion_detail,load_mode='append') #Changes for Jira::WMIT-5794 | to reduce number of partitions in dataframe
                xdpLogger('xDP-INF-111',comment="I-Records Inserted to table,Table : {}".format(app_config.file_ingestion_detail))
                return True
            else:
                xdpLogger("xDP-DBG-001",comment="List contains no entry for status table")
        except:
            xdpLogger("xDP-WAR-111",comment="Code went to exception section which could be generic AWS exceptions,To track the status of your job check file_ingestion_detail table")
            pass
    except Exception as e:
    #  xdpLogger("XDPEXP",e)
       tb = traceback.format_exc()
       xdpLogger("xDP-WAR-041",comment="Application id : {}, Exception Occurred :{}, traceback : {} ".format(application_id,e,tb))

def vernam_encrypt(plaintext):
    #global key
    if (plaintext=="" or plaintext is None):
        return ""
    print("###########")
    #plaintext=str(plaintext)
    #key=str(key)
    plaintext_bytearr = bytearray(plaintext,'utf-8')
    key1 = bytearray(key.decode('utf-8'),'utf-8')
    e = bytearray([ plaintext_bytearr[i] ^ key1[i%len(key1)] for i in range(len(plaintext_bytearr))])
    return str(base64.b64encode(e).decode('utf-8'))
  
def vernam_decrypt(ciphertext):
    if (ciphertext=="" or ciphertext is None):
        return ""
    print("$$$$$$$$$$$$$$$$$$$$$$")
    try:
        x=base64.b64decode(ciphertext)
        ciphertext_1 = bytearray(x)
        key1 = bytearray(key.decode(),'utf-8')
        decrypt_bytearr = bytearray([ ciphertext_1[i] ^ key1[i%len(key1)] for i in range(len(ciphertext_1))])
        return  decrypt_bytearr.decode('utf-8')              
    except:
        return "decrypt_fail"

def path_to_bucket_key(path):
    if not path.startswith('s3://'):
        raise ValueError('Given path is not a proper s3 path please check again: ' + path)
    path = path[5:].split('/')
    bucket = path[0]
    key = '/'.join(path[1:])
    return bucket, key

def get_Obfs_Key(spark):
    #This function gets the OBFUSCATION/DEOBFUSCATION Keys from the configuered DB
    get_logger()
    xdpLogger('xDP-INF-001')
    global key
    from fileingestion.config import obf_app_config
    session = botocore.session.get_session()
    client = session.create_client('s3')
    key_path=obf_app_config.key_path
    bucket, key = path_to_bucket_key(key_path)
    obj = client.get_object(Bucket=bucket, Key=key)
    key=obj['Body'].read()
    # obs_table=obf_app_config.obskey
    # query="select key from " + obs_table
    # key=frmspark.execute_query(query).first()[0]
    return str(key)


def parse_cli_args():
    # getting arguments from the command line
    msg = "File level checks and data type handling for File Ingestion"
    parser = argparse.ArgumentParser(description=msg)
    parser.add_argument('prefix', metavar='prefix', type=str, nargs=1,
                        help='Prefix for files to process for this run.')
    parser.add_argument('dq_flag', metavar='dq_flag', type=str, nargs=1,
                    help='Flag for DQ Checks.')

    args = parser.parse_args()
    prefix: str = args.prefix[0]
    dq_flag: str = args.dq_flag[0]
    return prefix, dq_flag


def main():
    create_logger(app_config.APPNAME)
    xdpLogger('xDP-INF-001')
    prefix, dq_flag = parse_cli_args()

    src_sys_nm, file_prefix_nm = utility.extract_Src_FilePrefix(prefix)

    print("SRC_SYS_NM: {}.".format(src_sys_nm))
    print("Prefix: {}.".format(file_prefix_nm))

    # TODO: Remove this
    global application_id
    application_id = frmspark.get_spark_app_id()
    msg = "Application ID: {}. Started.".format(application_id)
    xdpLogger("xDP-INF-111", comment=msg)
    msg = "Input Parameters: SRC_SYS_NM:{}. Prefix :{}."
    msg = msg.format(src_sys_nm, file_prefix_nm)
    xdpLogger('xDP-INF-111', comment=msg)

    preprocessing(src_sys_nm, file_prefix_nm)

    xdpLogger('xDP-INF-999')
    msg = "Application ID: {}. Success.".format(application_id)
    xdpLogger("xDP-INF-111", comment=msg)
    spark.stop()


if __name__ == "__main__":
    main()
