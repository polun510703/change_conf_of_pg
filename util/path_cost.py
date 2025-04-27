import os
import json
import re
import pandas as pd
from openpyxl import load_workbook

def get_log_search_alias(alias):
    if alias.startswith("TABLE_ITEMS_"):
        return "TABLE_ITEMS"
    return alias

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
def extract_path_info_from_log(log_path, needed_log_aliases):
    if not os.path.exists(log_path):
        print(f"[ERROR] Log file not found: {log_path}")
        return {}

    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        all_lines = f.readlines()

    alias_path_map = {a: [] for a in needed_log_aliases}

    current_log_alias = None
    collecting = False
    skip_until_next_block = False

    pattern = re.compile(r'^RELOPTINFO \(([^\)]*)\):')

    for line in all_lines:
        match = pattern.match(line)
        if match:
            collecting = False
            current_log_alias = None
            skip_until_next_block = False

            inside = match.group(1).strip()
            inside_aliases = inside.split()

            for inside_alias in inside_aliases:
                if inside_alias in needed_log_aliases:
                    current_log_alias = inside_alias
                    collecting = True
                    alias_path_map[current_log_alias].append(line.rstrip('\n'))
                    break

        else:
            if skip_until_next_block:
                continue

            if "cheapest parameterized paths" in line:
                skip_until_next_block = True
                continue

            if collecting and current_log_alias:
                alias_path_map[current_log_alias].append(line.rstrip('\n'))

    return alias_path_map


def output_path_cost_info(folder_path, log_filename):
    folder_path = "./path_analysis" 
    log_path = os.path.join(folder_path, log_filename)

    json_files = [f for f in os.listdir(folder_path) if f.endswith(".json")]

    if not os.path.exists(log_path):
        print(f"[ERROR] Unable to find log file: {log_path}")
        return

    for jf in json_files:
        full_path = os.path.join(folder_path, jf)
        aliases = parse_explain_json(full_path)

        if not aliases:
            print(f"[INFO] File: {jf} - No base relation aliases found, skipping subsequent path collection.")
            continue

        print(f"File: {jf}")
        print("  Base Relation Aliases:", ", ".join(sorted(aliases)))

        needed_log_aliases = set(get_log_search_alias(alias) for alias in aliases)

        alias_path_map = extract_path_info_from_log(log_path, needed_log_aliases)

        output_filename = jf.rsplit(".", 1)[0] + "_pathcost.txt"
        out_path = os.path.join(folder_path, output_filename)

        with open(out_path, "w", encoding="utf-8") as outf:
            for alias in sorted(aliases):
                parent_alias = get_log_search_alias(alias)
                path_lines = alias_path_map.get(parent_alias, [])
                if not path_lines:
                    continue
                outf.write(f"===== RELOPTINFO for alias: {alias} =====\n")
                for pl in path_lines:
                    outf.write(pl + "\n")
                outf.write("\n")

        print(f"  => Path information has been written to: {output_filename}\n")

    return out_path


def insert_blank_between_partitions(paths):
    unique_paths = []
    seen = set()

    for path in paths:
        key = (
            path["Scan Type"],
            path["Index Name"],
            path["Rows"],
            path["Startup Cost"],
            path["Total Cost"]
        )
        if key not in seen:
            seen.add(key)
            unique_paths.append(path)

    result = []
    first = True

    for path in unique_paths:
        if path["Scan Type"] == "SeqScan":
            if not first:
                result.append({
                    "Scan Type": None,
                    "Index Name": None,
                    "Rows": None,
                    "Startup Cost": None,
                    "Total Cost": None
                })
            first = False

        result.append(path)

    return result


def convert_pathcost_file_to_excel(input_file, output_excel):
    with open(input_file, "r", encoding="utf-8") as f:
        lines = f.readlines()

    reloptinfo_pattern = re.compile(r"^===== RELOPTINFO for alias: (.+?) =====$")
    path_list_pattern = re.compile(r"path list:")
    partial_path_list_pattern = re.compile(r"partial path list:")
    path_pattern = re.compile(
        r"^(IdxScan|SeqScan)\((.+?)\).*rows=(\d+).*cost=\s*([\d\.]+)\s*\.\.\s*([\d\.]+)"
    )
    index_name_pattern = re.compile(r"index name:\s*(\S+)")

    data = {}
    current_alias = None
    current_list_type = None
    last_path = None

    for line in lines:
        line = line.strip()
        if not line:
            continue

        reloptinfo_match = reloptinfo_pattern.match(line)
        if reloptinfo_match:
            alias = reloptinfo_match.group(1)
            if alias.startswith("TABLE_ITEMS_"):
                current_alias = "TABLE_ITEMS"
            else:
                current_alias = alias

            if current_alias not in data:
                data[current_alias] = {
                    "path_list": [],
                    "partial_path_list": []
                }
            current_list_type = None
            last_path = None
            continue

        if path_list_pattern.match(line):
            current_list_type = "path_list"
            last_path = None
            continue

        if partial_path_list_pattern.match(line):
            current_list_type = "partial_path_list"
            last_path = None
            continue

        if "required_outer" in line:
            continue

        path_match = path_pattern.match(line)
        if path_match and current_alias:
            scan_type, table, rows, startup_cost, total_cost = path_match.groups()
            try:
                rows = int(rows)
                startup_cost = float(startup_cost)
                total_cost = float(total_cost)
            except ValueError:
                print(f"[ERROR] Skipping invalid cost values: {startup_cost}..{total_cost}")
                continue

            if current_alias == "TABLE_ITEMS" and table != "TABLE_ITEMS":
                continue

            path = {
                "Scan Type": scan_type,
                "Index Name": None,
                "Rows": rows,
                "Startup Cost": startup_cost,
                "Total Cost": total_cost,
            }

            data[current_alias][current_list_type].append(path)
            last_path = path
            continue

        index_name_match = index_name_pattern.match(line)
        if index_name_match and last_path:
            index_name = index_name_match.group(1)
            last_path["Index Name"] = index_name
            continue

    with pd.ExcelWriter(output_excel) as writer:
        for alias, lists in data.items():
            sheet_data = [[f"Table: {alias}"]]

            df_path_list = pd.DataFrame(insert_blank_between_partitions(lists["path_list"]))
            df_partial_path_list = pd.DataFrame(insert_blank_between_partitions(lists["partial_path_list"]))

            if not df_path_list.empty:
                sheet_data.append(["Path List:"])
                sheet_data.append(["Scan Type", "Index Name", "Rows", "Startup Cost", "Total Cost"])
                sheet_data.extend(df_path_list.values.tolist())

            sheet_data.append([])

            if not df_partial_path_list.empty:
                sheet_data.append(["Partial Path List:"])
                sheet_data.append(["Scan Type", "Index Name", "Rows", "Startup Cost", "Total Cost"])
                sheet_data.extend(df_partial_path_list.values.tolist())

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