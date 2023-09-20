
import pandas as pd
import re
from universal_loader.web_utils import download_file
from universal_loader.generic_utils import unzip
import requests
from os.path import exists, join, basename
import os
import sys
from argparse import ArgumentParser


parser = ArgumentParser()
parser.add_argument('--start_date', default=None)
parser.add_argument('--end_date',default=None)
parser.add_argument('--time_horizon',default=None)
parser.add_argument('--file_types', default='all')


args = parser.parse_args()
if len(sys.argv[1:]) == 0:
    parser.print_help()
    # parser.print_usage() # for just the usage line
    parser.exit()

time_horizon_i = args.time_horizon
start_date_i = args.start_date
end_date_i = args.end_date
start_time = pd.Timestamp.now()

master_list="http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"

session=requests.session()

download_file(file_url=master_list,full_file_path="raw_data/masterfilelist.txt",session=session,overwrite_file=True)

# filter by arguments
df = pd.read_csv("raw_data/masterfilelist.txt",sep=" ",header=None,names=["ID","hash","url"])

ts_regex = "\d{14}"

def pull_date(url):
    try:
        ts=pd.Timestamp(re.findall(ts_regex,url)[0])
    except:
        ts = None
    return ts

df["ts"] = df["url"].apply(lambda x: pull_date(x))

if time_horizon_i is not None:
    # units of time horizon are days
    start_date = pd.Timestamp.now() - pd.Timedelta(value=int(time_horizon_i),unit="D")
    end_date = pd.Timestamp.now()

else:
    start_date = pd.Timestamp(start_date_i)
    end_date = pd.Timestamp(end_date_i)

# sanity check make sure end_date is bigger than start_date
if start_date > end_date:
    raise Exception("Invalid input start_date " + start_date_i + " should be bigger than end date " + end_date_i)


# find posting fitting horizon specified

latest_updates = df[(df["ts"] >= start_date) & (df["ts"] <= end_date)]

latest_updates.to_csv("latest_updates.csv",index=False)

num_files_to_process=len(latest_updates.index)

print("going to process " + str(num_files_to_process))

# download all the files to raw_data folder
for index, row in latest_updates.iterrows():
    url = row["url"]
    fname_no_zip = basename(url).split(".zip")[0]
    target_file = join("raw_data",basename(url))
    if not exists(join("raw_data",fname_no_zip)):
        print("Download file " + url)
        download_file(url,target_file, session)
        print("unzipping file " + target_file)
        unzip(file_name=target_file,target_folder="raw_data")
os.system("rm raw_data/*.zip")
end_time = pd.Timestamp.now()
elapsed_time = end_time - start_time
print(str(elapsed_time))








