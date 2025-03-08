import psycopg2
import os
import csv
import json
import itertools
import paramiko
import time
import pandas as pd
from util.config import db_config
from util.connection import send_query, get_pg_config, send_query_explain, Connection
from util.server import Server
from util.generate_insert_sql import generate_insert_statements


def generate_conf_json():
    query = "SHOW all;"
    conf_path = "./config/database.ini"
    params = db_config(conf_path)
    filename = "conf"
    path = "./config/db_conf.json"
    db_config_dict = get_pg_config(params=params)
    with open(path, 'w') as outputFile:
        outputFile.write("{\n")
        for key, value in db_config_dict.items():
            outputFile.writelines("\t\""+str(key)+"\":[\""+str(value)+"\"],\n")
        outputFile.write("}\n")

def dict_product(dicts):
    """
    >>> list(dict_product(dict(number=[1,2], character='ab')))
    [{'character': 'a', 'number': 1},
     {'character': 'a', 'number': 2},
     {'character': 'b', 'number': 1},
     {'character': 'b', 'number': 2}]
    """
    return (dict(zip(dicts, x)) for x in itertools.product(*dicts.values()))

def generate_all_possible_config(from_path="./config/db_conf.json"):
    with open(from_path , "r") as file:
        my_conf = json.load(file)   
        all_set = dict_product(dict(my_conf)) 
        # for set in all_set:
        #     for k,v in set.items():
        #         print("{0}='{1}'".format(k, v))
        return all_set
    
def write_file_on_server(file_name, content):
    with paramiko.SSHClient() as client:
        params = db_config(file_path="./config/database.ini", section='server')
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(**params)
        trans = client.get_transport()
        with paramiko.SFTPClient.from_transport(trans) as sftp:
            stdin , stdout, stderr = client.exec_command("touch {}".format(file_name))
            result = stdout.readlines()
            print(result)
            with sftp.file(file_name, "w") as file:
                file.write(content)
            stdin , stdout, stderr = client.exec_command("cat {}".format(file_name))
            result = stdout.readlines()
            error = stderr.readlines()
            print("result : {0} \n error : {1}".format(result[-3:-1], error))
    
        
            
def restart_postgresql():
    with paramiko.SSHClient() as client:
        params = db_config(file_path="./config/database.ini", section='server')
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(**params)
        stdin , stdout, stderr = client.exec_command("systemctl restart postgresql.service", get_pty=True)
        result = stdout.readlines()
        error = stderr.readlines()
        print("result : {0} \n error : {1}".format(result, error))
        if len(error) > 0:
            # systemctl status postgresql.service
            stdin , stdout, stderr = client.exec_command("systemctl status postgresql.service", get_pty=True)
            result = stdout.readlines()
            print("check : \n ")
            for i in result:
                print(i)
            result = [] # clean the result buffer
            error = stderr.readlines()
            print("result : {0} \n error : {1}".format(result, error))


def change_pg_conf(content):
    write_file_on_server("/var/lib/pgsql/data/postgresql.conf", content=content)
    restart_postgresql()
    time.sleep(1)

def get_sql_content(path):
    ret = ""
    with open(path, "r") as file:
        for i in file.readlines():
            ret+=i
        return ret

def get_sql_list(from_here):
    tmp_dct = {}
    for query in os.listdir(from_here):
        tmp_dct[query] = get_sql_content(from_here+"/"+query)
    return tmp_dct

def clean_cache():
    with paramiko.SSHClient() as client:
        params = db_config(file_path="./config/database.ini", section='server')
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(**params)
        stdin , stdout, stderr = client.exec_command("sh clean_pg_cache.sh", get_pty=True)
        result = stdout.readlines()
        error = stderr.readlines()
        print("result : {0} \n error : {1}".format(result, error))

def wait_for_cpu():
    with paramiko.SSHClient() as client:
        params = db_config(file_path="./config/database.ini", section='server')
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(**params, timeout=999)

        CPU_st = []
        while True:
            stdin, stdout, stderr = client.exec_command("sar -u 1 1 | awk '/^Average:/{print 100-$8}'", get_pty=True)
            result = stdout.readlines()
            error = stderr.readlines()
            if error:
                print("CPU check errors:", error)
            print(f"CPU result: {result}")
            CPU_st.append(float(result[0]))
            if len(CPU_st) > 10:
                CPU_st.pop(0)
            if len(CPU_st) == 10 and all(x < 20 for x in CPU_st):
                print("CPU is idle, checking disk I/O next...")
                break

        while True:
            stdin, stdout, stderr = client.exec_command("iotop-c -bo -d 3 -n 10 | grep 'Current DISK READ' | awk '{print $4, $5}'", get_pty=True)
            results = stdout.readlines()
            errors = stderr.readlines()
            if errors:
                print("Disk I/O errors:", errors)
            if not results:
                print("No Disk I/O data captured. Possible no activity or wrong command.")
                break
            
            io_data = []
            all_below_threshold = True
            for result in results:
                try:
                    value, unit = result.strip().split()
                    if 'B/s' in unit:
                        mbps = float(value) / (1024 * 1024)
                    elif 'K/s' in unit:
                        mbps = float(value) / 1024
                    elif 'M/s' in unit:
                        mbps = float(value)
                    else:
                        raise ValueError("Unknown unit for disk I/O")
                        break

                    io_data.append(f"{value} {unit} -> {mbps:.2f} MB/s")
                    if mbps >= 100:
                        all_below_threshold = False
                except ValueError as ve:
                    print("Error processing result:", ve)
            
            print("I/O Read Speeds (converted to MB/s):")
            for data in io_data:
                print(data)
                
            if all_below_threshold:
                print("System is ready for testing. CPU and Disk I/O are below thresholds.")
                return
            else:
                print("Disk I/O is too high, waiting for 3 seconds before rechecking...")
                time.sleep(3)
                continue
                
def get_timestamp():
    with paramiko.SSHClient() as client:
        params = db_config(file_path="./config/database.ini", section='server')
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(**params)
        stdin , stdout, stderr = client.exec_command("date +%s", get_pty=True)
        result = stdout.readlines()
        error = stderr.readlines()
        print("result : {0} \n error : {1}".format(result, error))
        return int(result[0])


def run_test(cold: bool, server: Server, iter_time=10, num_rows=300000, batch_size=10, combination_path="./config/db_conf.json", slower=False):
    report_path = "./report/report_{}".format(time.strftime("%Y-%m-%d-%H%M%S"))
    if not os.path.exists(report_path):
        os.mkdir(report_path)
    
    params = db_config("./config/database.ini")
    ori = ""
    with open("./config/default.conf", "r") as s:
        ori = s.read()
    
    for set in generate_all_possible_config(combination_path):
        content = ori
        conf_alter = ""
        for k, v in set.items():
            conf_alter += "{0}='{1}'\n".format(k, v)
        content += conf_alter
        
        change_pg_conf(content)
        # wait_for_cpu()
        
        total_time = 0
        report_dct = {
            "exec_time": [],
            "total_time": [],
            "timestamp": []
        }
        
        start_item_id = 2667044
        num_batches = num_rows // batch_size
        
        for i in range(iter_time):
            
            conn = Connection(params=params, query="")
            
            # autocommit
            conn.connect.autocommit = False
            
            report_dct["timestamp"].append(get_timestamp())
            
            if slower:
                server.start_record()
            
            start_time = time.time()
            
            try:
                with conn.connect.cursor() as cur:
                    for batch in range(num_batches):
                        batch_start_id = start_item_id + batch * batch_size
                        batch_insert_sql = generate_insert_statements(batch_size, batch_start_id)
                        # print("Generated SQL:\n", batch_insert_sql)
                        print(f"Inserting row #{batch}")
                        cur.execute(batch_insert_sql)
                        print(f"Insert row #{batch} successfully\n")
                        
                conn.connect.commit()
            
            except Exception as e:
                print(f"Unexpected error: {e}")
            
            end_time = time.time()
            
            exec_time = (end_time - start_time) * 1000
            
            print("exec:", exec_time, "ms")
            report_dct["exec_time"].append(int(exec_time))
            report_dct["total_time"].append(int(exec_time))
            
            if i != 0:
                total_time += int(exec_time)
            
            if slower:
                server.stop_record(report_path + "/bcc_insert_" + str(i) + ".csv")
        
        if iter_time > 1:
            total_time /= (iter_time - 1)
        
        folder_name = "insert_{}_rows_{}".format(num_rows, int(total_time))
        if cold:
            folder_name += "_Cold"
        else:
            folder_name += "_Warm"
        
        folder_dup = 0
        ori_folder_name = folder_name
        while os.path.exists(report_path + "/" + folder_name):
            folder_dup += 1
            folder_name = "{}_{}".format(ori_folder_name, folder_dup)
        
        small_report_path = report_path + "/" + folder_name
        os.mkdir(small_report_path)
        
        with open(small_report_path + "/conf.conf", "w") as conf_file:
            conf_file.writelines(conf_alter)
        
        df = pd.DataFrame(report_dct)
        df.to_csv(small_report_path + "/report.csv")

if __name__ == "__main__":
    s = Server('./config/database.ini')
    s.connect()
    if s.is_connect == False:
        print("ssh connection failed...")
    
    # the number of test iterations   
    iter_time = 2
    
    # the number of rows to insert
    num_rows = 300000
    
    # the number of rows to insert per batch
    batch_size = 1
    
    # check the version of PostgreSQL database you are going to test
    pg_major_version = s.get_postgresql_major_version()
    ready_to_test = False

    # Test SQL queries with different configurations on PostgreSQL 12
    if pg_major_version == 12:
        default_conf_path = "./config/db_conf_default_pg12.json"
        sunbird_conf_path = "./config/db_conf_sunbird_pg12.json"
        v5_conf_path = "./config/db_conf_v5_pg12.json"
        
        ready_to_test = True
    
    # Test SQL queries with different configurations on PostgreSQL 15
    elif pg_major_version == 15:
        default_conf_path = "./config/db_conf_default_pg15.json"
        sunbird_conf_path = "./config/db_conf_sunbird_pg15.json"
        v5_conf_path = "./config/db_conf_v5_pg15.json"
        
        ready_to_test = True
    
    ## 
    # Please check the configuration files when you are going to test SQL queries on PostgreSQL 12.
    # The database could be corrupted if you test using PostgreSQL 15 configurations on PostgreSQL 12.
    ##
    if ready_to_test:
        run_test(False, s, iter_time, num_rows, batch_size, sunbird_conf_path) # warm
    
    else:
        print("There might be an issue preventing the test from starting.")
        
    
    s.disconnect()
