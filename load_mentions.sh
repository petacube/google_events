data_folder=$PWD/raw_data
metadata_folder=$PWD/files_metadata
set -x

pattern="*.mentions.CSV"
# kernprof -l

gen_any_schema --target_environment postgres --folder_to_load $data_folder --file_pattern $pattern --target_table google_event_mentions  --header_file $metadata_folder/mentions_headers.txt --encoding utf-8 --incremental True --max_rows_to_sample 100



load_any_schema --target_environment postgres --folder_to_load $data_folder --file_pattern $pattern --target_table google_event_mentions --header_file $metadata_folder/mentions_headers.txt --encoding utf-8 --num_rows_to_sample 100
