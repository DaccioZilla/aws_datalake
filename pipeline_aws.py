from io import BytesIO
import urllib.request
import boto3
import os
from dotenv import load_dotenv
load_dotenv()

# Função para baixar dados de um URL e salvar em um arquivo
def extract_data(url, filename):
    try:
        urllib.request.urlretrieve(
            url, filename
        )  # Baixa o arquivo do URL e salva no local especificado
    except Exception as e:
        print(e)  # Imprime qualquer erro que ocorra durante o processo

# Chamando a função extract_data para baixar diferentes conjuntos de dados
# Cada conjunto de dados é salvo em um arquivo CSV diferente
extract_data("https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/c9509ab4-6f6d-4b97-979a-0cf2a10c922b/download/311_service_requests_2015.csv", "data/dados_2015.csv")
extract_data("https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/b7ea6b1b-3ca4-4c5b-9713-6dc1db52379a/download/311_service_requests_2016.csv", "data/dados_2016.csv")
extract_data("https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/30022137-709d-465e-baae-ca155b51927d/download/311_service_requests_2017.csv", "data/dados_2017.csv")
extract_data("https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/2be28d90-3a90-4af1-a3f6-f28c1e25880a/download/311_service_requests_2018.csv", "data/dados_2018.csv")
extract_data("https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/ea2e4696-4a2d-429c-9807-d02eb92e0222/download/311_service_requests_2019.csv", "data/dados_2019.csv")
extract_data("https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/6ff6a6fd-3141-4440-a880-6f60a37fe789/download/script_105774672_20210108153400_combine.csv", "data/dados_2020.csv")

# Lista dos nomes dos arquivos baixados
arquivos = [
    "data/dados_2015.csv",
    "data/dados_2016.csv",
    "data/dados_2017.csv",
    "data/dados_2018.csv",
    "data/dados_2019.csv",
    "data/dados_2020.csv",
]

# Dicionário para armazenar os dados de cada arquivo
dfs = {}

import pandas as pd

# Loop para ler cada arquivo e adicionar ao dicionário
for arquivo in arquivos:
    ano = arquivo.split("_")[-1].split(".")[0]  # Extrai o ano do nome do arquivo
    dfs[ano] = pd.read_csv(
        arquivo
    )  # Lê os dados do arquivo transforma em dataFrame e armazena no dicionário
# Exemplo de como acessar os dados de um ano específico
print(dfs["2018"].head())


# IMPORTANTE: Essas credenciais nao podem aparecer no seu codigo coloque em variaveis de ambiente
# exemplo: aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']

# TODO: Substituir <aws_access_key_id> e <aws_secret_access_key> pelas informações da sua conta de Armazenamento

aws_access_key_id = os.getenv('aws_access_key_id')
aws_secret_access_key = os.getenv('aws_secret_access_key')
region_name = os.getenv('region_name')
bucket_name = os.getenv('bucket_name')

boto3.setup_default_session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name,
)

# Criação de um cliente S3
s3 = boto3.client("s3")

content = """
Olá, S3!
"""

with open("hello-s3.txt", "w+") as f:
  f.write(content)

s3.upload_file("hello-s3.txt", bucket_name, "bronze/hello-s3")

for ano, df in dfs.items():

    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer)

    s3.put_object(
        Bucket= bucket_name,
        Key=f"bronze/dados_{ano}.parquet",
        Body=parquet_buffer.getvalue(),
    )

# Obtendo a lista de objetos do bucket
response = s3.list_objects(Bucket= bucket_name)

# Obtendo a lista de chaves dos objetos do bucket
keys = [obj["Key"] for obj in response["Contents"]]
print(keys)  # Imprime a lista de chaves