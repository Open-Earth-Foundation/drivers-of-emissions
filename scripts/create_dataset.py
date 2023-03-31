import openclimate
import os
import pandas as pd

if __name__ == "__main__":
    client = openclimate.Client()

    # output directory
    outputDir = '../data/processed'
    outputDir = os.path.abspath(outputDir)

    df_countries = client.parts('EARTH')[['actor_id', 'name']]

    df_pop = client.population(actor_id=list(df_countries['actor_id']), ignore_warnings=True)
    df_gdp = client.gdp(actor_id=list(df_countries['actor_id']), ignore_warnings=True)

    fl_emissions = '../data/processed/BP_review_2022_CO2_from_energy.csv'
    fl_emissions = os.path.abspath(fl_emissions)
    df_emissions = pd.read_csv(fl_emissions)

    fl_energy = '../data/processed/BP_review_2022_energy_consumption.csv'
    fl_energy = os.path.abspath(fl_energy)
    df_energy = pd.read_csv(fl_energy)

    fl_renewables = '../data/processed/BP_review_2022_renewable_consumption.csv'
    fl_renewables = os.path.abspath(fl_renewables)
    df_renewables = pd.read_csv(fl_renewables)

    fl_gas = '../data/processed/BP_review_2022_gas_consumption.csv'
    fl_gas = os.path.abspath(fl_gas)
    df_gas = pd.read_csv(fl_gas)

    fl_coal = '../data/processed/BP_review_2022_coal_consumption.csv'
    fl_coal = os.path.abspath(fl_coal)
    df_coal = pd.read_csv(fl_coal)

    columns = [
        'actor_id',
        'name',
        'year',
        'total_emissions',
        'population',
        'gdp',
        'energy_consumption_EJ',
        'renewable_consumption_EJ',
        'gas_consumption_EJ',
        'coal_consumption_EJ',
    ]

    df_tmp = (
        df_emissions[['actor_id', 'year','total_emissions']]
        .merge(df_pop[['actor_id', 'year', 'population']], on=['actor_id', 'year'])
        .merge(df_gdp[['actor_id', 'year', 'gdp']], on=['actor_id', 'year'])
        .merge(df_energy[['actor_id', 'year', 'energy_consumption_EJ']], on=['actor_id', 'year'])
        .merge(df_renewables[['actor_id', 'year', 'renewable_consumption_EJ']], on=['actor_id', 'year'])
        .merge(df_gas[['actor_id', 'year', 'gas_consumption_EJ']], on=['actor_id', 'year'])
        .merge(df_coal[['actor_id', 'year', 'coal_consumption_EJ']], on=['actor_id', 'year'])
        .merge(df_countries[['actor_id','name']], on=['actor_id'])
        .loc[:, columns]
    )

    df_out = df_tmp.assign(
        emissions_per_pop = lambda x: x['total_emissions'] / x['population'],
        gdp_per_pop = lambda x: x['gdp'] / x['population'],
        energy_per_gdp = lambda x: x['energy_consumption_EJ'] / x['gdp'],
        emissions_per_energy = lambda x: x['total_emissions'] / x['energy_consumption_EJ'],
        population_per_emissions = lambda x: x['population'] / x['total_emissions'],
        renewables_per_energy = lambda x: x['renewable_consumption_EJ'] / x['energy_consumption_EJ'],
        gas_per_energy = lambda x: x['gas_consumption_EJ'] / x['energy_consumption_EJ'],
        coal_per_energy = lambda x: x['coal_consumption_EJ'] / x['energy_consumption_EJ']
    ).reset_index()

    df_out.to_csv(f'{outputDir}/kaya_identity.csv', index=False)