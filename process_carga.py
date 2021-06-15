import numpy as np
import pandas as pd
from modules.DatabaseCon import Database


columns_carga_fato = [
    'IDCarga',
    'IDAtracacao',
    'Origem',
    'Destino',
    'CDMercadoria',
    'Tipo Operação da Carga',
    'Carga Geral Acondicionamento',
    'ConteinerEstado',
    'Tipo Navegação',
    'FlagAutorizacao',
    'FlagCabotagem',
    'FlagCabotagemMovimentacao',
    'FlagConteinerTamanho',
    'FlagLongoCurso',
    'FlagMCOperacaoCarga',
    'FlagOffshore',
    'FlagTransporteViaInterioir',
    'Percurso Transporte em vias Interiores',
    'Percurso Transporte Interiores',
    'STNaturezaCarga',
    'STSH2',
    'STSH4',
    'Natureza da Carga',
    'Sentido',
    'TEU',
    'QTCarga',
    'VLPesoCargaBruta',
    'Ano da data de início da operação da atracação',
    'Mês da data de início da operação da atracação',
    'Porto Atracação',
    'SGUF',
    'Peso líquido da carga'
]


def process_cargaFato(atracacao_df, carga_df, carga_Conteinerizada, columns=columns_carga_fato):
    atracacao_df['Data Início Operação'] = pd.to_datetime(atracacao_df['Data Início Operação'])
    atracacao_df['Ano da data de início da operação da atracação'] = atracacao_df['Data Início Operação'].dt.year
    atracacao_df['Mês da data de início da operação da atracação'] = atracacao_df['Data Início Operação'].dt.month
    atracacao_df.replace({np.nan: None}, inplace=True)

    carga_df.replace({np.nan: None}, inplace=True)

    carga_Conteinerizada.rename(columns={'VLPesoCargaConteinerizada': 'Peso líquido da carga'}, inplace=True)
    carga_Conteinerizada.replace({np.nan: None}, inplace=True)

    data_df = pd.merge(carga_df, atracacao_df, how='left', on='IDAtracacao')
    data_df = pd.merge(data_df, carga_Conteinerizada, how='left', on='IDCarga')
    conteinerizada = data_df['Natureza da Carga'] == 'Carga Conteinerizada'
    data_df.loc[conteinerizada, 'CDMercadoria'] = data_df.loc[conteinerizada, 'CDMercadoriaConteinerizada']
    data_df.loc[~conteinerizada, 'Peso líquido da carga'] = data_df.loc[~conteinerizada, 'VLPesoCargaBruta']
    data_df.replace({np.nan: None}, inplace=True)

    c_number = [
        'Peso líquido da carga',
        'VLPesoCargaBruta'
    ]

    data_df[c_number] = data_df[c_number].apply(lambda x: x.str.replace(',', '.'))

    return data_df[columns]


if __name__ == '__main__':
    year = input('Ano para processar: ')

    atracacao_df = pd.read_csv(f'data/{year}/{year}Atracacao.txt', delimiter=';')[['IDAtracacao', 'Porto Atracação', 'SGUF', 'Data Início Operação']]
    carga_df = pd.read_csv(f'data/{year}/{year}Carga.txt', delimiter=';')
    carga_Conteinerizada = pd.read_csv(f'data/{year}/{year}Carga_Conteinerizada.txt', delimiter=';')

    data_df = process_cargaFato(atracacao_df, carga_df, carga_Conteinerizada)

    con = Database(server='localhost,1433', database='dadosFato', uid='sa', pwd='Eng@Dados2021')
    con.insert_values('carga_fato', data_df)
