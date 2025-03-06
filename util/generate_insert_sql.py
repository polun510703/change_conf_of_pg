def generate_insert_statements(num_rows, start_id=1):
    insert_sql = "INSERT INTO \"dct_items\" (\"item_id\", \"item_name\", \"item_alias\", \"u_position\", \"layout_horiz_front\", \"layout_horiz_rear\", \"layout_vertical_left\", \"layout_vertical_right\", \"mounted_rails_pos_lks_id\", \"facing_lks_id\", \"cad_handle\", \"power_panels_count\", \"class_lks_id\", \"subclass_lks_id\", \"status_lks_id\", \"location_id\", \"parent_item_id\", \"model_id\", \"item_detail_id\", \"piq_id\", \"slot_position\", \"raritan_tag\", \"is_tag_verified\", \"layout_vertical_left_back\", \"layout_vertical_right_back\", \"free_data_port_count\", \"free_power_port_count\", \"free_input_cord_count\", \"shelf_position\", \"location_reference\", \"cad_name\", \"planned_decommission_date\", \"available_date\", \"planning_date\", \"layout_vertical_left_middle\", \"layout_vertical_right_middle\", \"valid\", \"reconciled\", \"proxy_index\", \"sub_location_id\", \"position_in_row\", \"elevation_aff\", \"ps_redundancy\", \"potential_power\", \"effective_power\", \"substatus_lku_id\")\nVALUES\n"
    
    values_list = []
    
    base_values = (
        "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', "
        "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', "
        "'9223372036854775807', "
        "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', "
        "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', "
        "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', "
        "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', "
        "'9223372036854775807', "
        "'9223372036854775807', "
        "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', "
        "'2147483647', "
        "'9223372036854775807', "
        "'9223372036854775807', "
        "'9223372036854775807', "
        "'9223372036854775807', "
        "'9223372036854775807', "
        "'9223372036854775807', "
        "'9223372036854775807', "
        "'9223372036854775807', "
        "'9223372036854775807', "
        "'aaaaaaaaaaaaaaaaaaaaaaaaa', "
        "TRUE, "
        "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', "
        "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', "
        "'2147483647', "
        "'2147483647', "
        "'2147483647', "
        "'2147483647', "
        "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', "
        "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', "
        "'2025-03-03', "
        "'2025-03-03', "
        "'2025-03-03', "
        "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', "
        "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', "
        "TRUE, "
        "TRUE, "
        "'9223372036854775807', "
        "'9223372036854775807', "
        "'2147483647', "
        "'12345678.1234', "
        "'32767', "
        "'2147483647', "
        "'2147483647', "
        "'9223372036854775807'"
    )
    
    for i in range(start_id, start_id + num_rows):
        values_list.append(f"('{i}', {base_values})")
    
    insert_sql += ",\n".join(values_list) + "\n"
    
    insert_sql += "ON CONFLICT (\"item_id\") DO UPDATE SET\n"
    insert_sql += ",\n".join([f"    \"{col}\" = EXCLUDED.\"{col}\"" for col in [
        "item_name", "item_alias", "u_position", "layout_horiz_front", "layout_horiz_rear", "layout_vertical_left", "layout_vertical_right", "mounted_rails_pos_lks_id", "facing_lks_id", "cad_handle", "power_panels_count", "class_lks_id", "subclass_lks_id", "status_lks_id", "location_id", "parent_item_id", "model_id", "item_detail_id", "piq_id", "slot_position", "raritan_tag", "is_tag_verified", "layout_vertical_left_back", "layout_vertical_right_back", "free_data_port_count", "free_power_port_count", "free_input_cord_count", "shelf_position", "location_reference", "cad_name", "planned_decommission_date", "available_date", "planning_date", "layout_vertical_left_middle", "layout_vertical_right_middle", "valid", "reconciled", "proxy_index", "sub_location_id", "position_in_row", "elevation_aff", "ps_redundancy", "potential_power", "effective_power", "substatus_lku_id"
    ]]) + ";"
    
    return insert_sql