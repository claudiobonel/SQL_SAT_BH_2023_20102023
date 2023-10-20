###################################################
#Criando um arquivo de banco de dados do SQLite
#É para ser executado em um script de criação de BD
###################################################
from pathlib import Path

Endereco = Path('./')

BD = Endereco / 'BD.db'

if Endereco.exists():
    if (BD.exists()):
        print ('Banco de dados já existe')
    else:
        BD.touch()
else:
    print ('Endereço não existe')


###########################################################################################
#Processo de extração, tratamento e carga de dados do site do INEP no Banco de dados SQLIte
###########################################################################################
#Bibliotecas
import pandas as pd #Manipulação de dados
import zipfile as zip #Manipulação de arquivos zipados
import requests #Coletar dados da web (Webscraping)
from io import BytesIO #Armazenar dados em Bytes na memória (Dar celeridade ao processo)
from datetime import datetime #Obter dados relacionados a datas e horas
import sqlite3 as sql #manipulação de dados no banco SQLite

#Abrindo uma conexão com o banco de dados
Conexao = sql.connect(Endereco / 'BD.db')

#URL Padrão: https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2021.zip
#colentando ano corrente
Ano_Atual = datetime.today().year

#coletando o histório de 3 anos, conforme solicitação do requisito
#Para disponibilizar a IHC - historico = int(input("Quantos anos de histórico? "))
Ano_historico = Ano_Atual-3

#índice para escolher a pasta a ser renomeada
i = 0

#Loop de carga de dados, considerando os 3 últimos anos
while Ano_historico < Ano_Atual: # <= inclui o ano atual
    #########################################
    # Extração de dados do site do INEP
    #########################################
    url = "http://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_{}.zip".format(Ano_historico) #forma antiga, ainda funciona

    #obtendo o arquio zip na url e transformando-o Arquivo de Bytes
    arquivoZip = BytesIO(
        requests.get(url,verify=False).content
        )
    MeuArquivoZip = zip.ZipFile(arquivoZip)

    #realiza a extração dos arquivos na pasta dados2021
    MeuArquivoZip.extractall("./dados")

    #Obtendo o nome das pastas descompactadas
    import os
    ListaPastas = os.listdir('./dados')

    #Renomeando as pastas compactadas
    #renames para pastas
    #rename para arquivos
    os.renames (f'./dados/{ListaPastas[i]}',f'./dados/censo_{Ano_historico}') #f string
    i += 1 #incrementa o índice para ajustar a listaPastas

    #Coletando dados CSV, extraídos e descompactados na pasta e armazenando-os em um DF Pandas
    enderecoCSV = f'./dados/censo_{Ano_historico}/dados/MICRODADOS_CADASTRO_CURSOS_{Ano_historico}.CSV' #f string
    df_dados = pd.DataFrame(pd.read_csv(enderecoCSV,encoding='ISO-8859-1',sep=';'))

    #########################################
    # Tratamento de dados
    #########################################
    #Definindo somente as colunas de interesse, de acordo com a regra de negócio
    df_dados = df_dados[["NU_ANO_CENSO","NO_REGIAO","SG_UF","NO_MUNICIPIO","NO_CURSO","TP_MODALIDADE_ENSINO","QT_ING_FEM","QT_ING_MASC"]]
    #df_dados

    #Substituir valores de modalidade de esino, de acordo com a regra de negócio
    df_dados['TP_MODALIDADE_ENSINO']=df_dados['TP_MODALIDADE_ENSINO'].replace({1:'Presencial',2:"EaD"})
    
    #Excluindo valores faltantes (Missing values). Excluir esser valores, melhora a performance.
    df_dados = df_dados.dropna()

    #Renomear o index
    df_dados.index.name = 'idINEP'
    
    ######################################
    #Carregando dados no banco de dados
    ######################################
    #Criando e inserindo dados
    #replace = substitui
    #append = adiciona
    
    df_dados.to_sql('tbINEP',Conexao,if_exists='append')


    #Mensagem de carga de dados
    print("------------------------------------------------------------------")
    print("Dados do ano de {} carregados com sucesso.".format(Ano_historico)) #forma antiga
    print("------------------------------------------------------------------")

    #Soma 1 ano ano histórico e retorna para mais um loop
    Ano_historico = Ano_historico + 1

#fechando conexão com o banco
Conexao.close()
print("Processo de extração e carga finalizado!")