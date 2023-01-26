import boto3
import pandas as pd

#Criar um cliente para interagir com a AWS S3
s3_client = boto3.client('s3')

#download
s3_client.download_file('nome do bucket',
                        'caminho_base_dados.csv',
                        'caminho_saida_base_dados.csv')

#leitura da base de dados
df = pd.read_csv("caminho_base_dados.csv", sep=";", encoding= 'latin-1', nrows= 10)
print(df)


#upload
s3_client.upload_file("caminho_base_dados.csv",
                    "nome do bucket da aws", 
                    "caminho_base_dados.csv")