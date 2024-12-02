import os, json, logging, sys, requests, unicodedata, pymongo, pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    regexp_extract, max as spark_max, col, when, input_file_name, lit, date_format, to_date
)
from pyspark.sql.types import (
    MapType, StringType, ArrayType, BinaryType, StructType, StructField, DoubleType, LongType
)
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus
from unidecode import unidecode
from pyspark.sql.functions import regexp_extract

def processing_reviews():
    try:
        # Inicializa a sessão Spark
        spark = SparkSession.builder.appName("CompassAggregation").getOrCreate()

        logging.info("Iniciando o processo de agregação de dados")

        # Carregar dados dos arquivos Parquet
        df_google_play = spark.read.parquet('/santander/silver/compass/reviews/googlePlay')
        df_mongodb = spark.read.parquet('/santander/silver/compass/reviews/mongodb')
        df_apple_store = spark.read.parquet('/santander/silver/compass/reviews/appleStore')

        # Adicionar nome do arquivo, se necessário
        df_google_play = df_google_play.withColumn("file_name", input_file_name())
        df_mongodb = df_mongodb.withColumn("file_name", input_file_name())
        df_apple_store = df_apple_store.withColumn("file_name", input_file_name())

        # Processar os dados e extrair a data (odate) do nome do arquivo
        df_google_play = df_google_play.withColumn("odate", regexp_extract("file_name", r"odate=(\d{8})", 1))
        df_mongodb = df_mongodb.withColumn("odate", regexp_extract("file_name", r"odate=(\d{8})", 1))
        df_apple_store = df_apple_store.withColumn("odate", regexp_extract("file_name", r"odate=(\d{8})", 1))

        # Verificar se os DataFrames foram carregados corretamente
        if df_google_play is not None:
            logging.info("Estrutura do DataFrame Google Play:")
            df_google_play.printSchema()
            df_google_play.show(5)
        else:
            logging.warning("Nenhum dado carregado para Google Play")

        if df_mongodb is not None:
            logging.info("Estrutura do DataFrame MongoDB:")
            df_mongodb.printSchema()
            df_mongodb.show(5)
        else:
            logging.warning("Nenhum dado carregado para MongoDB")

        if df_apple_store is not None:
            logging.info("Estrutura do DataFrame Apple Store:")
            df_apple_store.printSchema()
            df_apple_store.show(5)
        else:
            logging.warning("Nenhum dado carregado para Apple Store")

        # Verificar se algum dos DataFrames está vazio
        if df_google_play.count() == 0 and df_mongodb.count() == 0 and df_apple_store.count() == 0:
            logging.error("Nenhum dado foi carregado para unificar.")
            return None

        # Equalizando as colunas
        df_google_play_equalizado = df_google_play.select(
            col("id").alias("id"),
            col("app").alias("app"),
            col("rating").cast("string").alias("rating"), 
            col("iso_date").alias("iso_date"),
            col("title").alias("title"),
            col("snippet").alias("snippet"),
            col("historical_data").alias("historical_data"),
            col("odate").alias("odate"),
            col("file_name").alias("file_name")
        ).withColumn("app_source", lit("google_play"))

        df_mongodb_equalizado = df_mongodb.select(
            col("id").alias("id"),
            col("app").alias("app"),
            col("rating").cast("string").alias("rating"),
            col("timestamp").alias("iso_date"), 
            col("comment").alias("title"), 
            col("comment").alias("snippet"),
            col("historical_data").alias("historical_data"),
            col("odate").alias("odate"),
            col("file_name").alias("file_name")
        ).withColumn("app_source", lit("internal_database_mongodb"))

        df_apple_store_equalizado = df_apple_store.select(
            col("id").alias("id"),
            col("app").alias("app"),
            col("im_rating").cast("string").alias("rating"),
            col("updated").alias("iso_date"),
            col("title").alias("title"),
            col("content").alias("snippet"),
            col("historical_data").alias("historical_data"),
            col("odate").alias("odate"),
            col("file_name").alias("file_name")
        ).withColumn("app_source", lit("apple_store"))

        # Removendo a coluna 'historical_data' dos DataFrames
        df_google_play_equalizado_sem_historical = df_google_play_equalizado.drop("historical_data")
        df_mongodb_equalizado_sem_historical = df_mongodb_equalizado.drop("historical_data")
        df_apple_store_equalizado_sem_historical = df_apple_store_equalizado.drop("historical_data")



        # Unificar os DataFrames
        df_unificado = df_google_play_equalizado_sem_historical.union(df_mongodb_equalizado_sem_historical).union(df_apple_store_equalizado_sem_historical)


        # Verificar se o DataFrame unificado é válido e retorná-lo
        if df_unificado is not None:
            logging.info("Estrutura do DataFrame Unificado:")
            df_unificado.printSchema()
            df_unificado.show(5)
            return df_unificado  # Retorna o DataFrame unificado
        else:
            logging.error("Erro ao criar o DataFrame unificado.")
            return None

    except Exception as e:
        logging.error(f"Erro ao processar os dados: {e}")
        raise

    
def save_reviews(reviews_df: DataFrame, directory: str):
    """
    Salva os dados do DataFrame no formato Delta no diretório especificado.

    Args:
        reviews_df (DataFrame): DataFrame PySpark contendo as avaliações.
        directory (str): Caminho do diretório onde os dados serão salvos.
    """
    try:
        # Verifica se o diretório existe e cria-o se não existir
        Path(directory).mkdir(parents=True, exist_ok=True)

        # Escrever os dados no formato Delta
        # reviews_df.write.format("delta").mode("overwrite").save(directory)
        reviews_df.write.option("compression", "snappy").mode("overwrite").parquet(directory)
        logging.info(f"Dados salvos em {directory} no formato Delta")

    except Exception as e:
        logging.error(f"Erro ao salvar os dados: {e}")
        exit(1)


def save_dataframe(df, path, label):
    """
    Salva o DataFrame em formato parquet e loga a operação.
    """
    try:
        if df.limit(1).count() > 0:  # Verificar existência de dados
            logging.info(f"Salvando dados {label} para: {path}")
            save_reviews(df, path)
        else:
            logging.warning(f"Nenhum dado {label} foi encontrado!")
    except Exception as e:
        logging.error(f"Erro ao salvar dados {label}: {e}", exc_info=True)
        


def write_to_mongo(dados_feedback: dict, table_id: str, overwrite=False):
    """
    Escreve dados em uma coleção MongoDB, com a opção de sobrescrever a coleção.
    
    Args:
        dados_feedback (dict or list): Dados a serem inseridos na coleção (um único dicionário ou uma lista de dicionários).
        table_id (str): Nome da coleção onde os dados serão inseridos.
        overwrite (bool): Se True, sobrescreve a coleção, excluindo todos os documentos antes de inserir novos dados.
    """
    # Recuperar credenciais do MongoDB a partir das variáveis de ambiente
    mongo_user = os.environ["MONGO_USER"]
    mongo_pass = os.environ["MONGO_PASS"]
    mongo_host = os.environ["MONGO_HOST"]
    mongo_port = os.environ["MONGO_PORT"]
    mongo_db = os.environ["MONGO_DB"]

    # ---------------------------------------------- Escapar nome de usuário e senha ----------------------------------------------
    escaped_user = quote_plus(mongo_user)
    escaped_pass = quote_plus(mongo_pass)

    # ---------------------------------------------- Conexão com MongoDB ----------------------------------------------------------
    mongo_uri = f"mongodb://{escaped_user}:{escaped_pass}@{mongo_host}:{mongo_port}/{mongo_db}?authSource={mongo_db}&maxPoolSize=1"

    client = pymongo.MongoClient(mongo_uri)

    try:
        db = client[mongo_db]
        collection = db[table_id]

        # Se o parâmetro overwrite for True, exclui todos os documentos da coleção antes de inserir novos dados
        if overwrite:
            collection.delete_many({})  # Remove todos os documentos da coleção

        # Inserir dados no MongoDB
        if isinstance(dados_feedback, dict):  # Verifica se os dados são um dicionário
            collection.insert_one(dados_feedback)
        elif isinstance(dados_feedback, list):  # Verifica se os dados são uma lista
            collection.insert_many(dados_feedback)
        else:
            print("Os dados devem ser um dicionário ou uma lista de dicionários.")
    
    except Exception as e:
        print(f"Erro ao conectar ou inserir no MongoDB: {e}")
    finally:
        # Garante que a conexão será fechada
        client.close()


def define_schema() -> StructType:
    """
    Define o schema para os dados dos reviews.
    """
    return StructType([
        StructField("avatar", StringType(), True),
        StructField("date", StringType(), True),
        StructField("id", StringType(), True),
        StructField("iso_date", StringType(), True),
        StructField("likes", LongType(), True),
        StructField("rating", DoubleType(), True),
        StructField("response", MapType(StringType(), StringType(), True), True),
        StructField("snippet", StringType(), True),
        StructField("title", StringType(), True)
    ])

def read_data(spark: SparkSession, schema: StructType, pathSource: str) -> DataFrame:
    """
    Lê os dados de um caminho Parquet e retorna um DataFrame.
    """
    try:
        df = spark.read.schema(schema).parquet(pathSource) \
            .withColumn("app", regexp_extract(input_file_name(), "/googlePlay/(.*?)/odate=", 1)) \
            .drop("response")
        df.printSchema()
        df.show(truncate=False)
        return df
    except Exception as e:
        logging.error(f"Erro ao ler os dados: {e}", exc_info=True)
        raise

def save_data(valid_df: DataFrame, invalid_df: DataFrame):
    """
    Salva os dados válidos e inválidos nos caminhos apropriados.
    """

    # Definindo caminhos
    datePath = datetime.now().strftime("%Y%m%d")
    path_target = f"/santander/gold/compass/reviews/apps_santander_aggregate/odate={datePath}/"
    path_target_fail = f"/santander/gold/compass/reviews_fail/apps_santander_aggregate/odate={datePath}/"
    
    try:
        save_dataframe(valid_df, path_target, "valido")
        save_dataframe(invalid_df, path_target_fail, "invalido")
    except Exception as e:
        logging.error(f"Erro ao salvar os dados: {e}", exc_info=True)
        raise

def save_metrics(metrics_json: str):
    """
    Salva as métricas no MongoDB.
    """
    try:
        metrics_data = json.loads(metrics_json)
        write_to_mongo(metrics_data, "dt_datametrics_compass", overwrite=False)
        logging.info(f"Métricas da aplicação salvas: {metrics_json}")
    except json.JSONDecodeError as e:
        logging.error(f"Erro ao processar métricas: {e}", exc_info=True)


def save_data_gold(df, collection_name: str):
    """
    Sobrescreve os dados de uma coleção no MongoDB com o conteúdo de um DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame contendo os dados a serem inseridos.
        collection_name (str): Nome da coleção no MongoDB.
    """
    try:
        # Verifica se o DataFrame está vazio
        if df.count() == 0:
            logging.warning(f"A coleção '{collection_name}' não foi atualizada pois o DataFrame está vazio.")
            return
        
        # Converte o DataFrame em uma lista de dicionários
        #data = json.loads(df.to_json(orient="records"))
        # Converte o DataFrame do PySpark em uma lista de dicionários (JSON)
        data = [json.loads(row) for row in df.toJSON().collect()]


        # Salva no MongoDB
        write_to_mongo(data, collection_name, overwrite=True)
        
        logging.info(f"Dados da coleção '{collection_name}' atualizados com sucesso! {len(data)} documentos inseridos.")
    
    except Exception as e:
        logging.error(f"Erro ao sobrescrever a coleção '{collection_name}': {e}", exc_info=True)
