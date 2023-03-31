import csv
import os
import pandas as pd
from pathlib import Path
from typing import List
from typing import Dict


def make_dir(path=None):
    """Create a new directory at this given path.

    path = None
    """
    assert isinstance(path, str), (
        f"Path must be a string; you passed a {type(path)}"
    )

    # parents = True creates missing parent paths
    # exist_ok = True igores FileExistsError exceptions
    # these settings mimick the POSIX mkdir -p command
    Path(path).mkdir(parents=True, exist_ok=True)


def get_fieldnames(tableName=None, schema_json=None):
    """switcher to get field names for each table

    schema_json is a json file containing the openClimate schema
        {table_name : list_of_table_columns}
    """

    if schema_json is None:
        schema_json = '../resources/openClimate_schema.json'
        schema_json = os.path.abspath(schema_json)

    assert isinstance(schema_json, str), (
        f"schema_json must be a string; not a {type(schema_json)}"
    )

    # switcher stuff needs to be a JSON
    switcher = read_json(
        fl=schema_json)
    return switcher.get(tableName.lower(), f"{tableName} not in {list(switcher.keys())}")


def write_to_csv(outputDir=None,
                 tableName=None,
                 dataDict=None,
                 mode=None):

    # set default values
    outputDir = '.' if outputDir is None else outputDir
    tableName = 'Output' if tableName is None else tableName
    dataDict = {} if dataDict is None else dataDict
    mode = 'w' if mode is None else mode

    # ensure correct type
    assert isinstance(outputDir, str), f"outputDir must a be string"
    assert isinstance(tableName, str), f"tableName must be a string"
    assert isinstance(dataDict, dict), f"dataDict must be a dictionary"
    acceptableModes = ['r', 'r+', 'w', 'w+', 'a', 'a+', 'x']
    assert mode in acceptableModes, f"mode {mode} not in {acceptableModes}"

    # test that dataDict has all the necessary fields
    fieldnames_in_dict = [key in get_fieldnames(tableName) for key in dataDict]
    assert all(
        fieldnames_in_dict), f"Key mismatch: {tuple((dataDict.keys()))} != {get_fieldnames(tableName)}"

    # remove a trailing "/" in the path
    out_dir = Path(outputDir).as_posix()

    # create out_dir if does not exist
    make_dir(path=out_dir)

    # write to file
    with open(f'{out_dir}/{tableName}.csv', mode) as f:
        w = csv.DictWriter(f, fieldnames=get_fieldnames(tableName))

        # only write header once
        # this helped (https://9to5answer.com/python-csv-writing-headers-only-once)
        if f.tell() == 0:
            w.writeheader()

        w.writerow(dataDict)


def df_to_csv(df=None,
              outputDir=None,
              tableName=None):

    # set default values
    outputDir = '.' if outputDir is None else outputDir
    tableName = 'Output' if tableName is None else tableName

    # ensure correct type
    assert isinstance(df, pd.core.frame.DataFrame), f"df must be a DataFrame"
    assert isinstance(outputDir, str), f"outputDir must a be string"
    assert isinstance(tableName, str), f"tableName must be a string"

    # remove a trailing "/" in the path
    out_dir = Path(outputDir).as_posix()

    # create out_dir if does not exist
    make_dir(path=out_dir)

    df.to_csv(f'{out_dir}/{tableName}.csv', index=False)


def df_wide_to_long(df=None,
                    value_name=None,
                    var_name=None):

    # set default values (new column names)
    # new column name with {value_vars}
    var_name = "year" if var_name is None else var_name
    # new column name with values
    value_name = "values" if value_name is None else value_name

    # ensure correct type
    assert isinstance(df, pd.core.frame.DataFrame), f"df must be a DataFrame"
    assert isinstance(var_name, str), f"var_name must a be string"
    assert isinstance(value_name, str), f"value_name must be a string"

    # ensure column names are strings
    df.columns = df.columns.astype(str)

    # columns to use as identifiers (columns that are not number)
    id_vars = [val for val in list(df.columns) if not val.isdigit()]

    # columns to unpivot (columns that are numbers)
    value_vars = [val for val in list(df.columns) if val.isdigit()]

    # Unpivot (melt) a DataFrame from wide to long format
    df_long = df.melt(id_vars=id_vars,
                      value_vars=value_vars,
                      var_name=var_name,
                      value_name=value_name)

    # convert var_name column to int
    df_long[var_name] = df_long[var_name].astype(int)

    return df_long

def df_columns_as_str(df=None):
    df.columns = df.columns.astype(str)
    return df


def df_drop_nan_columns(df=None):
    return df.loc[:, df.columns.notna()]


def df_drop_unnamed_columns(df=None):
    return df.loc[:, ~df.columns.str.contains('^Unnamed')]