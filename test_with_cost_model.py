import psycopg2
import os
import csv
import json
import itertools
import paramiko
import time
import shutil
import fnmatch
import pandas as pd
from util.config import db_config
from util.connection import send_query, get_pg_config, send_query_explain, Connection
from util.server import Server
from util.path_cost import output_path_cost_info, delete_today_log_file, make_unique_dir

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

        # Step 1: Stop original PostgreSQL service
        stop_cmd = "sudo systemctl stop postgresql"
        print("[INFO] Stopping PostgreSQL...")
        stdin, stdout, stderr = client.exec_command(stop_cmd, get_pty=True)
        result = stdout.read().decode("utf-8", errors="replace")
        error = stderr.read().decode("utf-8", errors="replace")
        print(f"Stop result:\n{result}")
        if error:
            print(f"Stop error:\n{error}")

        # Step 2: Delete today's log file
        delete_today_log_file(client)

        # Step 3: Restart modified PostgreSQL service
        cmd = ("sudo -i -u postgres /usr/local/pgsql_mod/bin/pg_ctl -D /var/lib/pgsql/data stop; "
               "sudo -i -u postgres /usr/local/pgsql_mod/bin/pg_ctl -D /var/lib/pgsql/data start"
        )
        print("[INFO] Restarting PostgreSQL...")
        stdin, stdout, stderr = client.exec_command(cmd, get_pty=True)
        result = stdout.readlines()
        error = stderr.readlines()
        print("Restart result:\n", result)
        print("Restart error:\n", error)

        if len(error) > 0:
            print("[WARNING] Checking PostgreSQL status...")
            stdin, stdout, stderr = client.exec_command("systemctl status postgresql.service", get_pty=True)
            result = stdout.readlines()
            for line in result:
                print(line, end='')
            error = stderr.readlines()
            if error:
                print("Status error:\n", error)


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


def run_test(cold:bool, server:Server, iter_time=10, combination_path="./config/db_conf.json", slower=False):
    os.makedirs("./path_analysis", exist_ok=True)
    report_path = "./report/report_{}".format(time.strftime("%Y-%m-%d-%H%M%S"))
    if os.path.exists(report_path) == False:
        os.mkdir(report_path)
    query_path = "./raw_queries"
    query_dict = {}
    query_dict = get_sql_list(query_path)
    print("The following test queries are loaded :", query_dict.keys())
    params = db_config("./config/database.ini")
    ori = ""
    content = ""
    with open("./config/default.conf", "r") as s:
        for i in s.readlines():
            ori+=i
    for set in generate_all_possible_config(combination_path):
        content = ori
        conf_alter = ""
        for k, v in set.items():
            conf_alter+="{0}='{1}'\n".format(k, v)
        content+=conf_alter
        change_pg_conf(content)
        # wait_for_cpu()
        # start sending query
        # need to store explain and conf
        explain = ""
        for k, v in query_dict.items():
            total_time =0
            tmp_folder_name = str(k.split('.')[0])+"tmp"
            small_report_path = report_path+"/"+tmp_folder_name
            report_dct = {
                "sql":[],
                "exec_time":[],
                "plan_time":[],
                "total_time":[],
                "timestamp":[]
            }
            if os.path.exists(small_report_path) == False:
                os.mkdir(small_report_path)
            for i in range(iter_time):
                if cold == True : 
                    clean_cache()
                    wait_for_cpu()
                conn = Connection(params=params, query=v)
                # get the start time timestamp
                report_dct["timestamp"].append(get_timestamp())
                # start the ext4slower
                # pid = conn.get_pid()
                # server.start_record_pid(pid)
                if slower : 
                    server.start_record()
                time.sleep(1)
                # explain = send_query_explain(params, v) # dict
                explain = conn.get_explain_of_query() # dict
                explain_json = json.dumps(explain)
                print(k.split('.')[0], 
                      "exec : ",
                      explain['Execution Time'],"ms plan : ", 
                      explain['Planning Time'], "ms")
                report_dct["exec_time"].append(int(explain['Execution Time']))
                report_dct["plan_time"].append(int(explain['Planning Time']))
                report_dct["total_time"].append(int(explain['Execution Time'])+ int(explain['Planning Time']))
                report_dct["sql"].append(str(str(k.split('.')[0])+"_"+str(i)))
                if i != 0:
                    total_time += int(explain['Execution Time'])+ int(explain['Planning Time'])
                if os.path.exists(small_report_path+"/plan") == False:
                    os.mkdir(small_report_path+"/plan")
                with open(small_report_path+"/plan/"+str(k.split('.')[0])+"_"+str(i)+".json", "w") as plan_file:
                    plan_file.writelines(str(explain_json))
                
                # open the path_analysis folder and store the explain json file
                with open("./path_analysis/"+str(k.split('.')[0])+".json", "w") as plan_file:
                        plan_file.writelines(str(explain_json))
                
                # open the folder and store the bcc report (ext4slower)
                if os.path.exists(small_report_path+"/bcc") == False and slower:
                    os.mkdir(small_report_path+"/bcc")
                if slower :
                    server.stop_record(small_report_path+"/bcc/"+str(k.split('.')[0])+"_"+str(i)+".csv")
            if (iter_time == 1):
                total_time/=1
            else:
                total_time/=(iter_time-1)
            folder_name=str(k.split('.')[0])+"_"+str(int(total_time))
            if cold :
                folder_name+="_Cold"
            else:
                folder_name+="_Warm"
            # make sure the name of folder is valid
            folder_dup = 0
            ori_folder_name = folder_name
            while os.path.exists(report_path+"/"+folder_name) == True:
                folder_dup+=1
                folder_name = "{0}_{1}".format(ori_folder_name, folder_dup)
            os.rename(report_path+"/"+tmp_folder_name, report_path+"/"+folder_name)
            small_report_path = report_path+"/"+folder_name
            if os.path.exists(small_report_path) == False:
                os.mkdir(small_report_path)
            with open(small_report_path+"/conf.conf", "w") as conf_file:
                conf_file.writelines(conf_alter)
            # store the report
            df = pd.DataFrame(report_dct)
            # df_sorted = df.sort_values(by="total_time", ascending = False)
            df.to_csv(small_report_path+"/report.csv")
            # df_sorted.to_csv(small_report_path+"/report2.csv")
            

def tail_remote_log_and_output_path_cost(
    line_count,
    remote_log_dir="",
    local_out_dir=""
):
    os.makedirs(local_out_dir, exist_ok=True)

    # SSH/SFTP connection
    params = db_config(file_path="./config/database.ini", section='server')
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(**params)
    sftp = client.open_sftp()

    try:
        # Determine the day of the week for the log file name
        stdin, stdout, stderr = client.exec_command("date +%a", get_pty=True)
        day_of_week = stdout.read().decode("utf-8", errors="replace").strip()

        remote_log_path = f"{remote_log_dir}/postgresql-{day_of_week}.log"
        local_log_path  = os.path.join(local_out_dir,
                                       f"postgresql-{day_of_week}.log")

        # default: -1 means download the whole log file
        # if line_count is not 0, it will tail the log file
        if line_count == -1:
            print(f"[INFO] Downloading full log: {remote_log_path}")
            try:
                sftp.get(remote_log_path, local_log_path)
            except FileNotFoundError:
                print(f"[ERROR] Remote log not found: {remote_log_path}")
                return
        else:
            cmd = f"tail -n {line_count} {remote_log_path}"
            print(f"[INFO] Executing on server: {cmd}")
            stdin, stdout, stderr = client.exec_command(cmd, get_pty=True)
            out_data = stdout.read().decode("utf-8", errors="replace")
            err_data = stderr.read().decode("utf-8", errors="replace")
            if err_data:
                print(f"[ERROR] stderr from tail: {err_data}")
                return
            with open(local_log_path, "w", encoding="utf-8") as f:
                f.write(out_data)

        # Check if the log file was downloaded successfully
        if not os.path.exists(local_log_path):
            print(f"[ERROR] Local log not found: {local_log_path}")
            return

        # Move .json files to subdirectories
        # and copy the log file with a new name
        json_files = [f for f in os.listdir(local_out_dir)
                      if f.endswith(".json")]

        if not json_files:
            print("[WARNING] No .json files found to analyse.")
            return

        for jf in json_files:
            sql_name   = os.path.splitext(jf)[0]
            target_dir = make_unique_dir(
                os.path.join(local_out_dir, sql_name))

            # move the .json file to the new directory
            shutil.move(os.path.join(local_out_dir, jf),
                        os.path.join(target_dir, jf))

            # copy the log file to the new directory
            renamed_log = f"{sql_name}_postgresql-{day_of_week}.log"
            shutil.copyfile(local_log_path,
                            os.path.join(target_dir, renamed_log))

            # analyse the log file and output path cost info
            output_path_cost_info(
                target_dir,
                renamed_log,
                keep_in_place=True
            )
        
        # Remove the original log file if it exists
        for fname in os.listdir(local_out_dir):
            if fnmatch.fnmatch(fname, "postgresql-*.log"):
                target = os.path.join(local_out_dir, fname)
                try:
                    os.remove(target)
                    print(f"[INFO] Removed original log file: {target}")
                except OSError as e:
                    print(f"[WARNING] Could not remove {target}: {e}")

    finally:
        sftp.close()
        client.close()


if __name__ == "__main__":
    s = Server('./config/database.ini')
    s.connect()
    if s.is_connect == False:
        print("ssh connection failed...")
    
    # the number of test iterations   
    iter_time = 1
    
    # check the version of PostgreSQL database you are going to test
    pg_major_version = s.get_postgresql_major_version()
    ready_to_test = False

    # Test SQL queries with different configurations on PostgreSQL 12
    if pg_major_version == 12:
        sunbird_conf_path = "./config/db_conf_sunbird_pg12.json"
        
        ready_to_test = True
    
    # Test SQL queries with different configurations on PostgreSQL 15
    elif pg_major_version == 15:
        sunbird_conf_path = "./config/db_conf_sunbird_pg15.json"
        
        ready_to_test = True
    
    ## 
    # Please check the configuration files when you are going to test SQL queries on PostgreSQL 12.
    # The database could be corrupted if you test using PostgreSQL 15 configurations on PostgreSQL 12.
    ##
    if ready_to_test:
        run_test(False, s, iter_time, sunbird_conf_path) # warm
        
        # line_count is the number of lines to tail from the log file
        # remote_log_dir is the directory of the log file on the remote server
        # local_out_dir is the directory to save the output file on the local machine
        tail_remote_log_and_output_path_cost(
            line_count=-1,
            remote_log_dir="/var/lib/pgsql/data/log",
            local_out_dir="./path_analysis"
        )
    
    else:
        print("There might be an issue preventing the test from starting.")
        
    
    s.disconnect()
