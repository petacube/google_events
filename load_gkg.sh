data_folder=$PWD/ready_to_load
set -x

export PYTHON_PATH=.
python parse_gkg.py

gen_any_schema --target_environment postgres --folder_to_load $data_folder --file_pattern "*/*.CSV" --encoding utf-8 --num_rows_to_read 100

load_any_schema --target_environment postgres --folder_to_load $data_folder --file_pattern "*/*.CSV" --encoding utf-8 --num_rows_to_sample 100
