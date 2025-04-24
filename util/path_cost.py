import os
import json
import re
import pandas as pd
from openpyxl import load_workbook

##
#   Delete the current day's PostgreSQL log file on the server.
#   Assumes logs follow the pattern: postgresql-<Day>.log
##
def delete_today_log_file(client):
    # Get current day (e.g., Mon, Tue, Wed...)
    stdin, stdout, stderr = client.exec_command("date +%a", get_pty=True)
    day_of_week = stdout.read().decode("utf-8", errors="replace").strip()

    log_path = f"/var/lib/pgsql/data/log/postgresql-{day_of_week}.log"
    delete_cmd = f"rm -f {log_path}"

    print(f"[INFO] Deleting log file: {log_path}")
    stdin, stdout, stderr = client.exec_command(delete_cmd, get_pty=True)
    err = stderr.read().decode("utf-8", errors="replace")
    if err:
        print(f"[WARNING] Error when deleting log file: {err}")
    else:
        print("[INFO] Log file deleted successfully.")

##
#   Recursively traverse the plan tree.
#   If the node contains "Relation Name" and "Alias" fields,
#   it represents a base relation. Add its alias to aliases_set.
##
def get_base_relation_aliases(plan_node, aliases_set):
    if "Relation Name" in plan_node and "Alias" in plan_node:
        aliases_set.add(plan_node["Alias"])

    subplans = plan_node.get("Plans", [])
    for sp in subplans:
        get_base_relation_aliases(sp, aliases_set)

##
#   Read a single .json file (output of EXPLAIN in JSON format),
#   and return all base relation aliases set in the query.
##
def parse_explain_json(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Determine the structure based on PostgreSQL EXPLAIN JSON format:
    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict) and "Plan" in data[0]:
        plan_root = data[0]["Plan"]
    elif isinstance(data, dict) and "Plan" in data:
        plan_root = data["Plan"]
    else:
        print(f"[WARNING] {file_path} does not match the expected EXPLAIN JSON structure, returning an empty set.")
        return set()

    aliases = set()
    get_base_relation_aliases(plan_root, aliases)
    return aliases

##
#   Extract RELOPTINFO (ALIAS) blocks from postgresql tail log file for specified aliases.
#   log_path:   path to the log file
#   aliases:    set of aliases to search for
##
def extract_path_info_from_log(log_path, aliases):
    if not os.path.exists(log_path):
        print(f"[ERROR] Log file not found: {log_path}")
        return {}

    # Read the log file
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        all_lines = f.readlines()

    # Store path information for each alias
    alias_path_map = {a: [] for a in aliases}

    current_alias = None
    collecting = False
    skip_until_next_block = False

    # Pattern to match RELOPTINFO (xxxx):
    # e.g., "RELOPTINFO (TABLE_ITEMS): rows=1234 width=..."
    pattern = re.compile(r'^RELOPTINFO \(([^\)]*)\):')

    for line in all_lines:
        match = pattern.match(line)
        if match:
            # Found a new RELOPTINFO line
            # Stop collecting the previous block
            collecting = False
            current_alias = None
            skip_until_next_block = False

            inside = match.group(1).strip()  # e.g. "TABLE_ITEMS" or "TABLE_ITEMS ITEM_CLASSES"
            # If this RELOPTINFO contains only one alias (no spaces)
            # and it exists in the alias set, start collecting
            if ' ' not in inside and inside in aliases:
                current_alias = inside
                collecting = True
                alias_path_map[current_alias].append(line.rstrip('\n'))
                
            # Otherwise, do not collect
            
        else:
            if skip_until_next_block:
                continue
            
            # check if the line contains "cheapest parameterized paths"
            if "cheapest parameterized paths" in line:
                skip_until_next_block = True
                continue
            
            # Not a RELOPTINFO line
            if collecting and current_alias:
                # Continue appending as long as we are in the same block
                # Stop when a new RELOPTINFO line is encountered
                alias_path_map[current_alias].append(line.rstrip('\n'))
    
    return alias_path_map


def output_path_cost_info(folder_path, log_filename):
    folder_path = "./path_analysis" 
    log_path = os.path.join(folder_path, log_filename)      # the log file to search for path information

    # Search for .json files in the folder
    json_files = [f for f in os.listdir(folder_path) if f.endswith(".json")]

    if not os.path.exists(log_path):
        print(f"[ERROR] Unable to find log file: {log_path}")
        return

    # Analyze each JSON file to extract base aliases
    for jf in json_files:
        full_path = os.path.join(folder_path, jf)
        aliases = parse_explain_json(full_path)

        if not aliases:
            print(f"[INFO] File: {jf} - No base relation aliases found, skipping subsequent path collection.")
            continue

        print(f"File: {jf}")
        print("  Base Relation Aliases:", ", ".join(sorted(aliases)))

        # Extract corresponding alias path information from the log
        alias_path_map = extract_path_info_from_log(log_path, aliases)

        # Write results back to a file
        output_filename = jf.rsplit(".", 1)[0] + "_pathcost.txt"
        out_path = os.path.join(folder_path, output_filename)

        with open(out_path, "w", encoding="utf-8") as outf:
            for a in aliases:
                path_lines = alias_path_map.get(a, [])
                if not path_lines:
                    continue
                outf.write(f"===== RELOPTINFO for alias: {a} =====\n")
                for pl in path_lines:
                    outf.write(pl + "\n")
                outf.write("\n")

        print(f"  => Path information has been written to: {output_filename}\n")
    
    return out_path
        
        
def convert_pathcost_file_to_excel(input_file, output_excel):
    with open(input_file, "r", encoding="utf-8") as f:
        lines = f.readlines()

    # Define regular expressions
    reloptinfo_pattern = re.compile(r"^===== RELOPTINFO for alias: (.+?) =====$")
    path_list_pattern = re.compile(r"path list:")
    partial_path_list_pattern = re.compile(r"partial path list:")
    path_pattern = re.compile(
        r"^(IdxScan|SeqScan)\((.+?)\).*rows=(\d+).*cost=\s*([\d\.]+)\s*\.\.\s*([\d\.]+)"
    )
    index_name_pattern = re.compile(r"index name:\s*(\S+)")
    parameterized_path_pattern = re.compile(r"required_outer")

    # Initialize the structure to store data
    data = {}
    current_alias = None
    current_list_type = None
    last_path = None

    for line in lines:
        line = line.strip()
        if not line:
            continue  # Skip empty lines

        # Match RELOPTINFO
        reloptinfo_match = reloptinfo_pattern.match(line)
        if reloptinfo_match:
            current_alias = reloptinfo_match.group(1)
            
            # Debugging
            # print(f"[DEBUG] Processing table: {current_alias}")                   
            
            data[current_alias] = {
                "path_list": [],
                "partial_path_list": []
            }
            current_list_type = None  # Reset the current section
            continue

        # Detect path list and partial path list
        if path_list_pattern.match(line):
            current_list_type = "path_list"
            last_path = None  # Reset the last path
            
            # Debugging
            # print(f"[DEBUG] Entering path_list for table: {current_alias}")     
            continue
        
        elif partial_path_list_pattern.match(line):
            current_list_type = "partial_path_list"
            last_path = None  # Reset the last path
            
            # Debugging
            # print(f"[DEBUG] Entering partial_path_list for table: {current_alias}")   
            continue

        # Match IdxScan or SeqScan
        path_match = path_pattern.match(line)
        if path_match and current_alias:
            scan_type, table, rows, startup_cost, total_cost = path_match.groups()
            try:
                startup_cost = float(startup_cost)
                total_cost = float(total_cost)
            except ValueError:
                print(f"[ERROR] Skipping invalid cost values: {startup_cost}..{total_cost}")
                continue

            # Skip parameterized paths
            if "required_outer" in line:
                
                # Debugging
                # print(f"[DEBUG] Skipping parameterized path: {scan_type}, {table}, {rows}, {startup_cost}, {total_cost}")     
                continue

            path = {
                "Scan Type": scan_type,
                "Index Name": None,  # Default to None, to be updated later when index name is parsed
                "Rows": int(rows),
                "Startup cost": startup_cost,
                "Total cost": total_cost,
            }

            data[current_alias][current_list_type].append(path)
            
            # Debugging
            # print(f"[DEBUG] Added path: {scan_type}, {table}, {rows}, {startup_cost}, {total_cost}")      

            last_path = path  # Update the last path for potential index name
            continue

        # Match index name and append to the last path
        index_name_match = index_name_pattern.match(line)
        if index_name_match and last_path:
            index_name = index_name_match.group(1)
            last_path["Index Name"] = index_name
            
            # Debugging
            # print(f"[DEBUG] Added index name: {index_name} to last path")     
            continue

    # Check the parsed results
    # print(f"[DEBUG] Parsed data: {data}")     

    # Write data to Excel
    with pd.ExcelWriter(output_excel) as writer:
        for alias, lists in data.items():
            # Write table name in each worksheet
            sheet_data = [[f"Table: {alias}"]]
            df_path_list = pd.DataFrame(lists["path_list"])
            df_partial_path_list = pd.DataFrame(lists["partial_path_list"])

            # Add path_list to the worksheet
            if not df_path_list.empty:
                sheet_data.append(["Path List:"])
                # Add column headers
                sheet_data.append(["Scan Type", "Index Name", "Rows", "Startup Cost", "Total Cost"])
                sheet_data.extend(df_path_list.values.tolist())

            # Add an empty line as a separator
            sheet_data.append([])

            # Add partial_path_list to the worksheet
            if not df_partial_path_list.empty:
                sheet_data.append(["Partial Path List:"])
                # Add column headers
                sheet_data.append(["Scan Type", "Index Name", "Rows", "Startup Cost", "Total Cost"])
                sheet_data.extend(df_partial_path_list.values.tolist())

            # Write the complete data to the Excel file
            pd.DataFrame(sheet_data).to_excel(writer, index=False, header=False, sheet_name=alias)
            
    wb = load_workbook(output_excel)
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        column_widths = [12, 40, 10, 15, 15] 

        for i, width in enumerate(column_widths, start=1):
            col_letter = ws.cell(row=1, column=i).column_letter
            ws.column_dimensions[col_letter].width = width

    wb.save(output_excel)

    print(f"Excel file has been saved to {output_excel}")