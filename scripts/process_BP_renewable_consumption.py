import csv
import os
import openclimate
from pathlib import Path
import pandas as pd
import pycountry
from typing import List
from typing import Dict
from utils import make_dir
from utils import df_wide_to_long

# connect to OpenClimate
client = openclimate.Client()
client.jupyter

def no_duplicates(df, col):
    return ~df.duplicated(subset=[col]).any()

def country_lookup(name):
    try:
        return pycountry.countries.lookup(name).alpha_2
    except LookupError:
        return float('NaN')


def simple_write_csv(output_dir: str = None,
                     name: str = None,
                     rows: List[Dict] | Dict = None) -> None:

    if isinstance(rows, dict):
        rows = [rows]

    with open(f'{output_dir}/{name}.csv', mode='w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


if __name__ == '__main__':
    # output directory
    outputDir = '../data/processed'
    outputDir = os.path.abspath(outputDir)
    make_dir(path=Path(outputDir).as_posix())

    # raw data file path
    fl = '../data/raw/bp-stats-review-2022-all-data.xlsx'
    fl = os.path.abspath(fl)

    # =================================================================
    # process data
    # =================================================================
    xl = pd.ExcelFile(fl)
    sheets = xl.sheet_names

    sheet_name = 'Renewables Consumption - EJ'
    units = 'Exajoules (input-equivalent)'

    replace_dict = {
        'Trinidad & Tobago': 'Trinidad and Tobago',
        'China Hong Kong SAR': 'China',
        'Iran': 'Iran, Islamic Republic of'
    }

    drop_columns = ['2021.1', '2011-21', '2021.2']
    rename_columns = {f'{units}': 'country'}
    country_groups = ['Central America', 'Eastern Africa', 'Middle Africa', 'Western Africa']
    not_countries = ["Source:", "Notes:", "Growth", "Data ", "European Union", "OECD", "0.05%", "Other "]

    df = (
        pd.read_excel(fl, sheet_name=sheet_name, header=2)
        .drop(columns = drop_columns)
        .rename(columns = rename_columns)
        .loc[lambda x: x['country'].notnull()]
        .loc[lambda x: ~x['country'].str.contains('total', case=False)]
        .loc[lambda x: ~x['country'].isin(country_groups)]
        .loc[lambda x:  ~(x['country'].str.contains('|'.join(not_countries)))]
        .assign(country = lambda x: x.country.replace(replace_dict))
    )

    # reshape dataframe
    df = df_wide_to_long(df, value_name='renewable_consumption_EJ', var_name='year')

    # get actor_id from country name
    df_iso = pd.DataFrame(
        data=[(name, country_lookup(name)) for name in set(list(df.country))],
        columns=['country', 'actor_id']
    )

    if not_found:=list(df_iso.loc[df_iso['actor_id'].isnull(), 'country']):
        print(f"Countries not found: {not_found}")

    df_out = pd.merge(df, df_iso, on=['country'])

    # sum across actors, needed because we are including Hong Kong as part of China
    df_out = df_out.groupby(by=['actor_id', 'year']).sum(numeric_only=True).reset_index()

    # create datasource_id
    df_out['id'] = df_out.apply(
        lambda row: f"BP_review2022:renewable-consumption:{row['actor_id']}:{row['year']}", axis=1)

    # check types and sort
    astype_dict = {
        'id': str,
        'actor_id': str,
        'year': int,
        'renewable_consumption_EJ': float,
    }
    outColumns = astype_dict.keys()

    df_out = (
        df_out[outColumns]
        .astype(astype_dict)
        .sort_values(by=['actor_id', 'year'])
    )

    # sort by actor_id and year
    df_out = df_out.sort_values(by=['actor_id', 'year'])

    # convert to csv
    df_out.to_csv(f'{outputDir}/BP_review_2022_renewable_consumption.csv', index=False)