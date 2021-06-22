import pandas as pd
import numpy as np
import sys
import os
import time, glob
import json
import subprocess
import shutil
#START
print("[Start]: Start DMAAP To GCP Reconcilliation PipeLine")
print("[Info]: Starting DMAAP File Generation")
path = 'input/NC/'
folder = os.fsencode(path)
filenames = []
for file in os.listdir(folder):
    filename = os.fsdecode(file)
    if filename.endswith( ('.sql') ):
        filenames.append(filename)
filenames.sort()
for DMAAP_name_of_file in filenames:
    sql_file = open(r"input/NC/"+DMAAP_name_of_file)
    file_readline = sql_file.readlines()
    for iterations in file_readline:
        if 'v_export_id' in iterations:
            tableName=iterations.split('=')
            tableName=tableName[1]
            tableName=str(tableName)
            remove_comma_tableName=tableName.replace("'","")
            tableNameFinal=remove_comma_tableName.replace("; ","")
            tableNameFF=tableNameFinal.replace("namm_vfhu_","")
            tableNameFinal1=tableNameFF.replace("\n","").upper()
            remove_nrbm=tableNameFinal1.replace("NRBM_","").upper()
            remove_nrdb=remove_nrbm.replace("NRDB_","").upper()
        if 'v_sql_query' in iterations:
            query=iterations.split('=')
            query=query[1]
            query=str(query)
            remove_from_query=query.find(" FROM ")
            remove_from_query=query[:remove_from_query]
            remove_from_query=str(remove_from_query)
            remove_select_statement=remove_from_query.replace("' SELECT ","").upper()
            remove_select_statement=remove_select_statement.replace("'SELECT ","").upper()
            remove_select_statement_f=remove_select_statement.strip()
            break
    with open("out/temp/"+DMAAP_name_of_file+".txt", "w",encoding='utf8') as outfile:
        outfile.seek(0)
        outfile.write("{}|{}\n".format(remove_nrdb,remove_select_statement_f))
outfilename =  "out/all_merged.csv"
filenames1 = glob.glob('out/temp/*.txt')
with open(outfilename, 'w') as outfile:
    outfile.seek(0) # go back to the beginning of the file
    outfile.write("TARGET_TABLE_NAME|SRC_FIELDS\n")
    for fname in filenames1:
        with open(fname, 'r') as readfile:
            infile = readfile.read()
            for line in infile:
                outfile.write(line)
            #outfile.write("")
print("[Info]: DMAAP File Generation Successfull")
print("[Info]: Starting GCP File(YML, DDL) Generation")
path2 = 'input/YML/'
folder2 = os.fsencode(path2)
filenames2 = []
for file1 in os.listdir(folder2):
    filename1 = os.fsdecode(file1)
    if filename1.endswith( ('.yaml') ):
        filenames2.append(filename1)
filenames2.sort()
for GCP_name_of_file in filenames2:
    yml_file = open(r"input/YML/"+GCP_name_of_file)
    file_yml = yml_file.readlines()
    outfile3= open("out/S.csv", "a",encoding='utf8')
    outfile3.seek(0)
    outfile3.write("TABLE_NUMBER|TARGET_TABLE_NAME\n")
    for iterations1 in file_yml:
        if 'bq.table' in iterations1:
            tableName_initial=iterations1.replace("'bq.table':","'bq.table'=")
            tableName=tableName_initial.split("=")
            tableName=tableName[1]
            tableName=str(tableName)
            remove_colon_table=tableName.replace(":","")
            remove_last_comma_tb=remove_colon_table.replace(",\n","")
            remove_first_commas_tb=remove_last_comma_tb.replace("'","").upper()
            outfile3= open("out/S.csv", "a",encoding='utf8')
            outfile3.write("1|{}\n".format(remove_first_commas_tb))
        if 'header' in iterations1:
            header_initial=iterations1.replace("'header':","'header'=")
            header=header_initial.split("=")
            header=header[1]
            header=str(header)
            remove_colon=header.replace(":","")
            remove_last_comma=remove_colon.replace(",\n","")
            remove_first_commas=remove_last_comma.replace("'","").upper()
path3 = 'input/YML/'
folder3 = os.fsencode(path2)
filenames3 = []
for file2 in os.listdir(folder3):
    filename2 = os.fsdecode(file2)
    if filename2.endswith( ('.yaml') ):
        filenames3.append(filename2)
filenames3.sort()
for GCP1_name_of_file in filenames3:
    yml_file1 = open(r"input/YML/"+GCP1_name_of_file)#Change_To_Dynamic
    file_yml1 = yml_file1.readlines()
    outfile2= open("out/S1.csv", "a",encoding='utf8')
    outfile2.seek(0)
    outfile2.write("TABLE_NUMBER|SRC_FIELDS\n")
    for iterations2 in file_yml1:
        if 'header' in iterations2:
            header_initial=iterations2.replace("'header':","'header'=")
            header=header_initial.split("=")
            header=header[1]
            header=str(header)
            remove_colon=header.replace(":","")
            remove_last_comma=remove_colon.replace(",\n","")
            remove_first_commas=remove_last_comma.replace("'","").upper()
            outfile2= open("out/S1.csv", "a",encoding='utf8')
            outfile2.write("1|{}\n".format(remove_first_commas))
    #print (remove_first_commas)
input1= pd.read_csv(r'out/S.csv',sep='|',encoding='utf8')#
input1.reset_index(inplace=True, drop=True)
input1.reset_index(inplace=True)
input1.drop('TABLE_NUMBER', axis=1, inplace=True)
input1.to_csv("out/sd.txt", index=False, sep="|")
input2= pd.read_csv(r'out/S1.csv',sep='|',encoding='utf8')#
input2.reset_index(inplace=True, drop=True)
input2.reset_index(inplace=True)
input2.drop('TABLE_NUMBER', axis=1, inplace=True)
input2.to_csv("out/sd1.txt", index=False, sep="|")
row1 = pd.read_csv('out/sd.txt',encoding='utf8',sep="|")
row2 = pd.read_csv('out/sd1.txt',encoding='utf8',sep="|")
out2 = pd.merge(row1, row2,
				on='index',
				how='inner')
out2.drop('index', axis=1, inplace=True)
out2.to_csv("out/all_yml_data.csv", index=False, sep="|")
print("[Info]: GCP File Generation(YML, DDL) Successfull")
print("[Info]: Starting Matching of HEADERS Between SQL and YML")
time.sleep(1)
print("[Info]: Starting Matching of HEADERS Between SQL and DDL")
time.sleep(1)
print("[Info]: Starting Matching of HEADERS Between YML and DDL")
time.sleep(1)
with open("out/all_json.csv", "a") as filea:
    filea.write("TARGET_TABLE_NAME|SRC_FIELDS\n")
    filea.seek(0)
for file in os.listdir('input/DDL/'):
    with open('input/DDL/' + file) as f:
        info = json.load(f)
    sys.stdout = open('out/all_json.csv','a')
    print(file + ' | ', end='')
    count = 0
    for elem in info:
        count += 1
        print(elem['name'], end=',') if count < len(info) else print(elem['name'])
read_json= pd.read_csv(r'out/all_json.csv',sep='|',encoding='utf8')
read_json['TARGET_TABLE_NAME']=read_json['TARGET_TABLE_NAME'].str.replace(" ","")
read_json['SRC_FIELDS']=read_json['SRC_FIELDS'].str.replace(" ","")
read_json['TARGET_TABLE_NAME']=read_json['TARGET_TABLE_NAME'].str.replace(".json","")
read_json['TARGET_TABLE_NAME']=read_json['TARGET_TABLE_NAME'].str.upper()
read_json.to_csv("out/all_json_trimmed.csv", index=False, sep="|")
read_json_1= pd.read_csv(r'out/all_json_trimmed.csv',sep='|',encoding='utf8')
outfile2.close()
outfile3.close()
sys.stdout.close()
os.remove(r'out/S.csv')
os.remove(r'out/S1.csv')
os.remove(r'out/sd.txt')
os.remove(r'out/sd1.txt')
subprocess.call([r'src\vf_hu_join_checker\vf_hu_join_checker_run.bat'])
os.remove(r'out/all_json.csv')
os.remove(r'out/all_merged.csv')
os.remove(r'out/all_yml_data.csv')
os.remove(r'out/all_json_trimmed.csv')
files_to_delete = glob.glob('out/temp/*')
for fl in files_to_delete:
    os.remove(fl)
original = r'out/Results'
target = r'Results/'
shutil.move(original,target)
#END