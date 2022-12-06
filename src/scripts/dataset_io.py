from os import path, rename
import numpy as np
import pandas as pd


def getDatasetFilePath(dataDir: str, countryName: str, fallback: str) -> str:
    if path.isfile(f'{dataDir}/{countryName}.pkl'):
        return f'{dataDir}/{countryName}.pkl'
    return f'{dataDir}/{fallback}.csv'


def sanitizeDataset(dataDir: str, settings: 'dict[str, any]') -> None:
    countryName: str = settings['countryName']
    indicator: str = settings['indicator']
    relevantCols: 'tuple[str]' = tuple(
        [str(elem) for elem in range(1990, 2023)]
        + ['Country Name', 'Indicator Code', 'Attribute']
    )
    df: pd.DataFrame = pd \
        .read_csv(
            settings['filePath'],
            low_memory=False,
            index_col=[0]) \
        .reset_index() \
        .query("`Country Name` == @countryName & `Indicator Code` == @indicator & Attribute == 'Value'") \
        .drop(['Country Code', 'Country Name', 'Indicator Code', 'Attribute'], axis=1)
    df: pd.DataFrame = df \
        .loc[:, df.columns.str.startswith(relevantCols)] \
        .transpose() \
        .reset_index() \
        .rename(columns={'index': 'ym'})

    df[['year', 'month']] = df['ym'].str.split('M', n=1, expand=True)
    df = df.drop(['ym'], axis=1)
    df = df.rename(columns={df.columns[0]: 'cpi'})
    df['year'] = pd.to_numeric(df['year'], downcast='integer')
    df['cpi'] = pd.to_numeric(df['cpi'], downcast='float')
    df['month'] = pd.to_numeric(df['month'], downcast='integer')

    # save to disk in pickle for better performance
    pd.to_pickle(df, f'{dataDir}/{countryName}.pkl')


def getDataset(dataDir: str, settings: 'dict[str, any]') -> pd.DataFrame:
    print('Reading from ' + settings['filePath'] + ' ...')
    if ('pkl' not in settings['filePath']):
        sanitizeDataset(dataDir, settings)
    df: pd.DataFrame = pd.read_pickle(
        f'{dataDir}/' + settings['countryName'] + '.pkl'
    )
    print('Done!')
    return df


if __name__ == '__main__':
    dataDir: str = '../../data'
    countryName = 'United Kingdom'

    settings: 'dict[str, any]' = {
        'filePath': getDatasetFilePath(
            dataDir,
            countryName,
            fallback='CPITimeSeries'
        ),
        'countryName': countryName,
        'indicator': 'PCPI_IX',
    }

    df = getDataset(dataDir, settings)
