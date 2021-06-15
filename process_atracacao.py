import numpy as np
import pandas as pd
from modules.DatabaseCon import Database


columns_atracacao_fato = [
    'IDAtracacao',
    'CDTUP',
    'IDBerco',
    'Berço',
    'Porto Atracação',
    'Apelido Instalação Portuária',
    'Complexo Portuário',
    'Tipo da Autoridade Portuária',
    'Data Atracação',
    'Data Chegada',
    'Data Desatracação',
    'Data Início Operação',
    'Data Término Operação',
    'Ano da data de início da operação',
    'Mês da data de início da operação',
    'Tipo de Operação',
    'Tipo de Navegação da Atracação',
    'Nacionalidade do Armador',
    'FlagMCOperacaoAtracacao',
    'Terminal',
    'Município',
    'UF',
    'SGUF',
    'Região Geográfica',
    'Nº da Capitania',
    'Nº do IMO',
    'TEsperaAtracacao',
    'TEsperaInicioOp',
    'TOperacao',
    'TEsperaDesatracacao',
    'TAtracado',
    'TEstadia'
]

c_number = [
    'TEsperaAtracacao',
    'TEsperaInicioOp',
    'TOperacao',
    'TEsperaDesatracacao',
    'TAtracado',
    'TEstadia'
]


def process_atracacaoFato(atracacao_df, temposAtracacao_df, columns=columns_atracacao_fato):
    for c in ['Data Atracação', 'Data Chegada', 'Data Desatracação', 'Data Início Operação', 'Data Término Operação']:
        atracacao_df[c] = pd.to_datetime(atracacao_df[c])
    atracacao_df['Ano da data de início da operação'] = atracacao_df['Data Início Operação'].dt.year
    atracacao_df['Mês da data de início da operação'] = atracacao_df['Data Início Operação'].dt.month

    data_df = pd.merge(atracacao_df, temposAtracacao_df, how='left', on='IDAtracacao')[columns]
    data_df.replace({np.nan: None}, inplace=True)
    data_df[c_number] = data_df[c_number].apply(lambda x: x.str.replace(',', '.'))

    return data_df


if __name__ == '__main__':
    year = input('Ano para processar: ')

    print('Lendo os arquivos...')
    atracacao_df = pd.read_csv(f'data/{year}/{year}Atracacao.txt', delimiter=';')
    temposAtracacao_df = pd.read_csv(f'data/{year}/{year}TemposAtracacao.txt', delimiter=';')

    print('Processando...')
    data_df = process_atracacaoFato(atracacao_df, temposAtracacao_df)

    print('Salvando no banco de dados...')
    con = Database(server='localhost,1433', database='dadosFato', uid='sa', pwd='Eng@Dados2021')
    con.insert_values('atracacao_fato', data_df)
