import pyodbc


class Database:
    def __init__(self, server, database, uid, pwd):
        try:
            str_connec = r'DRIVER={ODBC Driver 17 for SQL Server};' +\
                         'SERVER={};'.format(server) +\
                         'DATABASE={};PWD={};'.format(database, pwd)
            if uid:
                str_connec = str_connec + 'UID={};Trusted_Connection=no;'.format(uid)
            else:
                str_connec = str_connec + 'Trusted_Connection=yes;'
            self.connection = pyodbc.connect(str_connec)
            print('Conectado ao SQL Server')
            self.success = True
            if self.connection is not None:
                self.connection.autocommit = True
            self.cur = self.connection.cursor()

            self.cur.execute("SELECT table_name FROM information_schema.tables;")
            tab = self.cur.fetchall()
            tab = [t[0] for t in tab]
            if not ('atracacao_fato' in tab):
                self.cur.execute(create_atracacao_fato)
            if not ('carga_fato' in tab):
                self.cur.execute(create_carga_fato)
        except :
            print('Conexão com SQL Server falhou')
            self.success = False

    def insert_values(self, table, data_df, batch_size=500):
        if self.success:
            columns = data_df.columns.values
            str_insert = "INSERT INTO {} ({}) values({})".\
                format(table, ', '.join(['['+c+']' for c in columns]), ('?,'*len(columns))[:-1])
            for i in range(0, data_df.shape[0], batch_size):
                print(f'[{i}/{data_df.shape[0]}]')
                self.cur.executemany(str_insert, list(map(tuple, data_df.iloc[i:i + batch_size][columns].values)))
            print('Concluído')
        else:
            print('Sem Conexão com SQL Server')


create_atracacao_fato = '''
CREATE TABLE atracacao_fato (
IDAtracacao int NOT NULL PRIMARY KEY,
CDTUP VARCHAR(255),
IDBerco VARCHAR(255),
Berço VARCHAR(255),
[Porto Atracação] VARCHAR(255),
[Apelido Instalação Portuária] VARCHAR(255),
[Complexo Portuário] VARCHAR(255),
[Tipo da Autoridade Portuária] VARCHAR(255),
[Data Atracação] DATETIME,
[Data Chegada] DATETIME,
[Data Desatracação] DATETIME,
[Data Início Operação] DATETIME,
[Data Término Operação] DATETIME,
[Ano da data de início da operação] SMALLINT,
[Mês da data de início da operação] TINYINT,
[Tipo de Operação] VARCHAR(255),
[Tipo de Navegação da Atracação] VARCHAR(255),
[Nacionalidade do Armador] VARCHAR(255),
[FlagMCOperacaoAtracacao] VARCHAR(255),
Terminal VARCHAR(255),
Município VARCHAR(255),
UF VARCHAR(255),
SGUF VARCHAR(255),
[Região Geográfica] VARCHAR(255),
[Nº da Capitania] VARCHAR(255),
[Nº do IMO] VARCHAR(255),
TEsperaAtracacao FLOAT(20),
TEsperaInicioOp FLOAT(20),
TOperacao FLOAT(20),
TEsperaDesatracacao FLOAT(20),
TAtracado FLOAT(20),
TEstadia FLOAT(20)
);
'''

create_carga_fato = '''
CREATE TABLE carga_fato (
IDCarga INT NOT NULL,
IDAtracacao INT FOREIGN KEY REFERENCES atracacao_fato(IDAtracacao),
Origem VARCHAR(7),
Destino VARCHAR(7),
CDMercadoria VARCHAR(4),
[Tipo Operação da Carga] VARCHAR(255),
[Carga Geral Acondicionamento] VARCHAR(20),
ConteinerEstado VARCHAR(5),
[Tipo Navegação] VARCHAR(20),
FlagAutorizacao VARCHAR(1),
FlagCabotagem TINYINT,
FlagCabotagemMovimentacao TINYINT,
FlagConteinerTamanho VARCHAR(10),
FlagLongoCurso TINYINT,
FlagMCOperacaoCarga TINYINT,
FlagOffshore TINYINT,
FlagTransporteViaInterioir TINYINT,
[Percurso Transporte em vias Interiores] VARCHAR(40),
[Percurso Transporte Interiores] VARCHAR(40),
STNaturezaCarga VARCHAR(20),
STSH2 VARCHAR(20),
STSH4 VARCHAR(20),
[Natureza da Carga] VARCHAR(30),
Sentido VARCHAR(20),
TEU FLOAT(20),
QTCarga INT,
VLPesoCargaBruta FLOAT(20),
[Ano da data de início da operação da atracação] SMALLINT,
[Mês da data de início da operação da atracação] TINYINT,
[Porto Atracação] VARCHAR(255),
SGUF VARCHAR(2),
[Peso líquido da carga] FLOAT(20)
);
'''