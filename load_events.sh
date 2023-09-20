data_folder=$PWD/raw_data
set -x

data_to_load='*.export.CSV'

gen_any_schema --target_environment postgres --folder_to_load $data_folder --file_pattern $data_to_load --target_table google_events --header_file files_metadata/event_table_headers.txt --encoding utf-8 --num_rows_to_read 100 --max_rows_to_sample 100
load_any_schema --target_environment postgres --folder_to_load $data_folder --file_pattern $data_to_load --target_table google_events --header_file files_metadata/event_table_headers.txt --encoding utf-8 --num_rows_to_sample 100

