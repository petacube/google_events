import pandas as pd
from os.path import basename,join, exists

"""
Petacube 2019 Google Events parser
"""

def parse_complex_field(
    df,
    field_name,
                        offset=False,
                        file_header=True,
                        record_delim=";",
                        field_delim="#",
    output_folder="/tmp",
    output_prefix=""):
    result_list=[]
    df_clean = df[field_name].dropna()
    if file_header:
        file_headers = pd.read_csv(join("files_metadata","gkg_" + field_name + ".txt"), header=None)[0].values
    else:
        file_headers = [field_name]

    if offset:
        file_headers = file_headers + ["DOCUMENT_OFFSET"]

    file_headers = ['GKGRECORDID'] + list(file_headers)
    for index_value, val in df_clean.iteritems():
        count_list = val.split(record_delim)
        for cnt_rec in count_list:
            cnt_rec_values = cnt_rec.split(field_delim)
            if (len(cnt_rec_values) + 1) != len(file_headers):
                continue
            result_list.append([index_value] + cnt_rec_values)
    if len(result_list) >0:
        result_df = pd.DataFrame(result_list,columns=file_headers)
        result_df.to_csv(join(output_folder,output_prefix, field_name + ".CSV"), index=False)


def parse_simple_field(df,field_name, field_delim=";",output_folder="/tmp",output_prefix=""):
    result_list=[]
    df_clean = df[field_name].dropna()
    v1_themes_headers = ['GKGRECORDID'] + [field_name]

    v1_themes_df = pd.DataFrame(columns=v1_themes_headers)

    # iterate
    for index_value, val in df_clean.iteritems():
        count_list = val.split(field_delim)
        for cnt_value in count_list:
            result_list.append([index_value] + [cnt_value])
            #rec_series = pd.Series([index_value] + [cnt_value], index=v1_themes_headers)
            #v1_themes_df = v1_themes_df.append(rec_series, ignore_index=True)
    v1_themes_df = pd.DataFrame(result_list,columns = v1_themes_headers)
    v1_themes_df.to_csv(join(output_folder, output_prefix, field_name + ".CSV"), index=False)

fields_to_process=[
'V1COUNTS',
'V2.1COUNTS',
"V1THEMES",
"V2ENHANCEDTHEMES",
"V1LOCATIONS",
"V2ENHANCEDLOCATIONS",
"V1PERSONS",
"V2ENHANCEDPERSONS",
"V1ORGANIZATIONS",
"V2ENHANCEDORGANIZATIONS",
"V1.5TONE",
"V2.1ENHANCEDDATES",
"V2GCAM",
"V2.1RELATEDIMAGES",
"V2.1SOCIALIMAGEEMBEDS",
"V2.1QUOTATIONS",
"V2.1ALLNAMES",
"V2.1AMOUNTS",
"V2.1TRANSLATIONINFO",
"V2EXTRASXML"
]

