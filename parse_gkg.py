import pandas as pd
import glob
from os.path import basename,join, exists
from os import mkdir,listdir
from gkg_parse_lib import parse_complex_field, parse_simple_field, fields_to_process
from dask.distributed import Client
import sys


def process_one_file(func_args):
    import pandas as pd
    import glob
    from os.path import basename, join, exists
    from os import mkdir, listdir
    from gkg_parse_lib import parse_complex_field, parse_simple_field, fields_to_process
    from dask.distributed import Client
    import sys

    fname = func_args[0]
    input_folder="raw_data"
    output_folder="ready_to_load"

    output_prefix = basename(fname).split(".")[0]
    print("processing " + fname)
    if not exists(join(output_folder, output_prefix)):
        mkdir(join(output_folder, output_prefix))
    try:
        df = pd.read_csv(fname, sep="\t", names=None, header=None, encoding="latin1")
    except:
        print("can't read the file " + fname)
        return

    gkg_headers = pd.read_csv("files_metadata/gkg_headers.txt", header=None)[0].values
    df.columns = gkg_headers
    df.set_index('GKGRECORDID', inplace=True)

    parse_complex_field(df=df, field_name='V1COUNTS', output_folder=output_folder, output_prefix=output_prefix)
    parse_complex_field(df=df, field_name='V2.1COUNTS', output_folder=output_folder, output_prefix=output_prefix)
    parse_simple_field(df=df, field_name="V1THEMES", output_folder=output_folder, output_prefix=output_prefix)
    parse_complex_field(df=df, field_name="V2ENHANCEDTHEMES", offset=True, file_header=False,
                        output_folder=output_folder, output_prefix=output_prefix)
    parse_complex_field(df=df, field_name="V1LOCATIONS", output_folder=output_folder, output_prefix=output_prefix)
    parse_complex_field(df=df, field_name="V2ENHANCEDLOCATIONS", offset=True, file_header=True,
                        output_folder=output_folder, output_prefix=output_prefix)
    parse_simple_field(df=df, field_name="V1PERSONS", output_folder=output_folder, output_prefix=output_prefix)
    parse_complex_field(df=df, field_name="V2ENHANCEDPERSONS",
                        file_header=False,
                        offset=True,
                        record_delim=";",
                        field_delim=",", output_folder=output_folder, output_prefix=output_prefix)

    parse_simple_field(df=df, field_name="V1ORGANIZATIONS", output_folder=output_folder, output_prefix=output_prefix)

    parse_complex_field(df=df, field_name="V2ENHANCEDORGANIZATIONS",
                        file_header=False,
                        offset=True,
                        record_delim=";",
                        field_delim=",", output_folder=output_folder, output_prefix=output_prefix)
    parse_complex_field(df=df, field_name="V1.5TONE", file_header=True, field_delim=',', output_folder=output_folder,
                        output_prefix=output_prefix)
    parse_complex_field(df=df,
                        field_name="V2.1ENHANCEDDATES",
                        file_header=True,
                        offset=False,
                        record_delim=";",
                        field_delim=",", output_folder=output_folder, output_prefix=output_prefix)

    parse_complex_field(df=df, field_name="V2GCAM", file_header=False, record_delim=',', field_delim=':',
                        output_folder=output_folder, output_prefix=output_prefix)
    parse_simple_field(df=df, field_name="V2.1RELATEDIMAGES", output_folder=output_folder, output_prefix=output_prefix)
    parse_simple_field(df=df, field_name="V2.1SOCIALIMAGEEMBEDS", output_folder=output_folder,
                       output_prefix=output_prefix)
    parse_complex_field(df=df, field_name="V2.1QUOTATIONS", record_delim='#', field_delim='|',
                        output_folder=output_folder, output_prefix=output_prefix)
    parse_complex_field(df=df, field_name="V2.1ALLNAMES",
                        record_delim=";",
                        field_delim=',',
                        offset=True,
                        file_header=False,
                        output_folder=output_folder,
                        output_prefix=output_prefix)

    parse_complex_field(df=df, field_name="V2.1AMOUNTS", file_header=True,
                        record_delim=";", field_delim=',',
                        output_folder=output_folder, output_prefix=output_prefix)

    parse_simple_field(df=df, field_name="V2.1TRANSLATIONINFO", output_folder=output_folder,
                       output_prefix=output_prefix)

    df.drop(columns=fields_to_process, inplace=True)

    df.to_csv(join(output_folder, output_prefix, "gkg.CSV"))



input_folder="raw_data"
output_folder="ready_to_load"

start_period=int(sys.argv[1])
end_period=int(sys.argv[2])

file_list=glob.glob(join(input_folder,"*.gkg.csv"))



folders_already_processed = set(listdir(output_folder))

# compute delta

files_with_dates = [(int(basename(fname).split(".")[0][0:8]), fname) for fname in file_list]
 
#delta_files = list(map(lambda x: x[1],filter(lambda x: x[0] not in folders_already_processed, files_with_dates)))

# filter by start and end dates
delta_files= list(map(lambda x: x[1], filter(lambda x: x[0] >= start_period and x[0] <= end_period,files_with_dates)))


# connect client
client = None
if len(sys.argv) ==4:
    # distributed processing
    client = Client(sys.argv[3])
else:
    # local multiprocesing
    client = Client()

func_args = [(f, input_folder,output_folder) for f in delta_files]

remote_status = client.map(process_one_file,func_args)
result = client.gather(remote_status)




