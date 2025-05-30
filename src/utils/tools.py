import os, json, logging, sys, requests, unicodedata, pymongo, pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    regexp_extract, max as spark_max, col, when, input_file_name, lit, date_format, to_date, regexp_replace
)
from pyspark.sql.types import (
    MapType, StringType, ArrayType, BinaryType, StructType, StructField, DoubleType, LongType, IntegerType
)
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus
from unidecode import unidecode
from pyspark.sql.functions import regexp_extract
from elasticsearch import Elasticsearch
from pyspark.sql.window import Window


def log_error(e, df=None, sufix=0):
    """Gera e salva métricas de erro no Elastic."""

    # Convertendo "segmento" para uma lista de strings, se df for válido
    if sufix == 1 and df is not None:
        segmentos_unicos = [row["segmento"] for row in df.select("segmento").distinct().collect()]
    else:
        segmentos_unicos = "NA"

    error_metrics = {
        "timestamp": datetime.now().isoformat(),
        "layer": "gold",
        "project": "compass",
        "job": "aggregate_apps_reviews",
        "priority": "0",
        "tower": "SBBR_COMPASS",
        "client": segmentos_unicos,
        "error": str(e)
    }

    # Serializa para JSON e salva no MongoDB
    save_metrics_job_fail(json.dumps(error_metrics))


def processing_reviews(spark, path_google_play, path_mongodb, path_apple_store):
    try:

        logging.info("[*] Iniciando o processo de agregação de dados")

        # Carregar dados dos arquivos Parquet
        df_google_play = spark.read.parquet(path_google_play)
        df_mongodb = spark.read.parquet(path_mongodb)
        df_apple_store = spark.read.parquet(path_apple_store)

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
            logging.info("[*] Estrutura do DataFrame Google Play:")
            df_google_play.printSchema()
            df_google_play.show(5)
        else:
            logging.warning("[*] Nenhum dado carregado para Google Play")

        if df_mongodb is not None:
            logging.info("[*] Estrutura do DataFrame MongoDB:")
            df_mongodb.printSchema()
            df_mongodb.show(5)
        else:
            logging.warning("[*] Nenhum dado carregado para MongoDB")

        if df_apple_store is not None:
            logging.info("[*] Estrutura do DataFrame Apple Store:")
            df_apple_store.printSchema()
            df_apple_store.show(5)
        else:
            logging.warning("[*] Nenhum dado carregado para Apple Store")

        # Verificar se algum dos DataFrames está vazio
        countGoogle = df_google_play.count()
        countMongo = df_mongodb.count()
        countApple = df_apple_store.count()
        if  countGoogle == 0 and countMongo == 0 and countApple == 0:
            logging.error("[*] Nenhum dado foi carregado para unificar.")
            log_error(e, spark.createDataFrame([], StructType([])), 0)

            return None

        # Equalizando as colunas
        df_google_play_equalizado = df_google_play.select(
            col("id").alias("id"),
            regexp_replace(col("app"), "_pf|_pj", "").alias("app"),
            col("segmento").alias("segmento"),
            col("rating").cast("int").cast("string").alias("rating"),
            col("iso_date").alias("iso_date"),
            col("title").alias("title"),
            col("snippet").alias("snippet"),
            col("odate").alias("odate"),
            col("file_name").alias("file_name")
        ).withColumn("app_source", lit("google_play"))

        df_mongodb_equalizado = df_mongodb.select(
            col("id").alias("id"),
            regexp_replace(col("app"), "_pf|_pj", "").alias("app"),
            col("segmento").alias("segmento"),
            col("rating").cast("string").alias("rating"),
            col("timestamp").alias("iso_date"), 
            col("comment").alias("title"), 
            col("comment").alias("snippet"),
            col("odate").alias("odate"),
            col("file_name").alias("file_name")
        ).withColumn("app_source", lit("internal_database_mongodb"))

        df_apple_store_equalizado = df_apple_store.select(
            col("id").alias("id"),
            regexp_replace(col("app"), "_pf|_pj", "").alias("app"),
            col("segmento").alias("segmento"),
            col("im_rating").cast("string").alias("rating"),
            col("updated").alias("iso_date"),
            col("title").alias("title"),
            col("content").alias("snippet"),
            col("odate").alias("odate"),
            col("file_name").alias("file_name")
        ).withColumn("app_source", lit("apple_store"))


        # Definição das colunas a serem selecionadas
        cols = ["id", "app", "segmento", "rating", "iso_date", "title", "snippet", "app_source"]

        # Unindo os DataFrames (sem distinct)
        df_unificado = (
            df_google_play_equalizado.select(cols)
            .union(df_mongodb_equalizado.select(cols))
            .union(df_apple_store_equalizado.select(cols))
        )

        # Criando uma janela particionada pelo "id" para remover duplicatas de forma eficiente
        window_spec = Window.partitionBy("id").orderBy(F.lit(1))

        df_final = (
            df_unificado.withColumn("row_number", F.row_number().over(window_spec))
            .filter(F.col("row_number") == 1)
            .drop("row_number")
        )


        # Verificar se o DataFrame unificado é válido e retorná-lo
        if df_final is not None:
            logging.info("[*] Estrutura do DataFrame Unificado:")
            df_final.printSchema()
            df_final.show(5)
            return df_final
        else:
            logging.error("[*] Erro ao criar o DataFrame unificado.")
            return None

    except Exception as e:
        logging.error(f"[*] Erro ao processar os dados: {e}")
        raise


def save_reviews(reviews_df: DataFrame, directory: str):
    """
    Salva os dados do DataFrame no formato Parquet no diretório especificado.

    Args:
        reviews_df (DataFrame): DataFrame PySpark contendo as avaliações.
        directory (str): Caminho do diretório onde os dados serão salvos.
    """
    try:
        # Verifica se o diretório existe e cria-o se não existir
        Path(directory).mkdir(parents=True, exist_ok=True)

        reviews_df.write.option("compression", "snappy").mode("overwrite").parquet(directory)
        logging.info(f"[*] Dados salvos em {directory} no formato Parquet")

    except Exception as e:
        logging.error(f"[*] Erro ao salvar os dados: {e}")
        log_error(e, reviews_df, 1)


def save_dataframe(df, path, label):
    """
    Salva o DataFrame em formato parquet e loga a operação.
    """
    try:

        if df.limit(1).count() > 0:  # Verificar existência de dados
            logging.info(f"[*] Salvando dados {label} para: {path}")
            save_reviews(df, path)
        else:
            logging.warning(f"[*] Nenhum dado {label} foi encontrado!")
    except Exception as e:
        logging.error(f"[*] Erro ao salvar dados {label}: {e}", exc_info=True)
        log_error(e, df, 1)
        


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
            print("[*] Os dados devem ser um dicionário ou uma lista de dicionários.")
    
    except Exception as e:
        print(f"[*] Erro ao conectar ou inserir no MongoDB: {e}")
    finally:
        # Garante que a conexão será fechada
        client.close()


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
        log_error(e, spark.createDataFrame([], StructType([])), 0)
        raise

def save_data(spark: SparkSession, valid_df: DataFrame, invalid_df: DataFrame,path_target: str,  path_target_fail: str):
    """
    Salva os dados válidos e inválidos nos caminhos apropriados.
    """
    try:
        save_dataframe(valid_df, path_target, "valido")
        save_dataframe(invalid_df, path_target_fail, "invalido")
    except Exception as e:
        logging.error(f"[*] Erro ao salvar os dados: {e}", exc_info=True)
        log_error(e, valid_df, 1)
        raise

def save_metrics(metrics_json):
    """
    Salva as métricas.
    """

    ES_HOST = "http://elasticsearch:9200"
    ES_INDEX = "compass_dt_datametrics"
    ES_USER = os.environ["ES_USER"]
    ES_PASS = os.environ["ES_PASS"]

    # Conectar ao Elasticsearch
    es = Elasticsearch(
        [ES_HOST],
        basic_auth=(ES_USER, ES_PASS)
    )

    try:
        # Converter JSON em dicionário
        metrics_data = json.loads(metrics_json)

        # Inserir no Elasticsearch
        response = es.index(index=ES_INDEX, document=metrics_data)

        logging.info(f"[*] Métricas da aplicação salvas no Elasticsearch: {response}")
    except json.JSONDecodeError as e:
        logging.error(f"[*] Erro ao processar métricas: {e}", exc_info=True)
        # JSON de erro
        error_metrics = {
            "timestamp": datetime.now().isoformat(),
            "layer": "gold",
            "project": "compass",
            "job": "reviews_aggregate_reviews",
            "priority": "3",
            "tower": "SBBR_COMPASS",
            "client": "NA",
            "error": str(e)
        }

        metrics_json = json.dumps(error_metrics)

        # Salvar métricas de erro no Elastic
        save_metrics_job_fail(metrics_json)


def save_data_mongo(df, collection_name: str):
    """
    Sobrescreve os dados de uma coleção no MongoDB com o conteúdo de um DataFrame.

    Args:
        df (pd.DataFrame): DataFrame contendo os dados a serem inseridos.
        collection_name (str): Nome da coleção no MongoDB.
    """
    try:
        # Verifica se o DataFrame está vazio
        if df.count() == 0:
            logging.warning(f"[*] A coleção '{collection_name}' não foi atualizada pois o DataFrame está vazio.")
            return

        # Converte o DataFrame do PySpark em uma lista de dicionários (JSON)
        data = [json.loads(row) for row in df.toJSON().collect()]

        # Salva no MongoDB
        write_to_mongo(data, collection_name, overwrite=True)

        logging.info(f"[*] Dados da coleção '{collection_name}' atualizados com sucesso! {len(data)} documentos inseridos.")

    except Exception as e:
        logging.error(f"[*] Erro ao sobrescrever a coleção '{collection_name}': {e}", exc_info=True)
        log_error(e, df, 1)





def save_metrics_job_fail(metrics_json):
    """
    Salva as métricas de aplicações com falhas
    """

    ES_HOST = "http://elasticsearch:9200"
    ES_INDEX = "compass_dt_datametrics_fail"
    ES_USER = os.environ["ES_USER"]
    ES_PASS = os.environ["ES_PASS"]

    # Conectar ao Elasticsearch
    es = Elasticsearch(
        [ES_HOST],
        basic_auth=(ES_USER, ES_PASS)
    )

    try:
        # Converter JSON em dicionário
        metrics_data = json.loads(metrics_json)

        # Inserir no Elasticsearch
        response = es.index(index=ES_INDEX, document=metrics_data)

        logging.info(f"[*] Métricas da aplicação salvas no Elasticsearch: {response}")
    except json.JSONDecodeError as e:
        logging.error(f"[*] Erro ao processar métricas: {e}", exc_info=True)
    except Exception as e:
        logging.error(f"[*] Erro ao salvar métricas no Elasticsearch: {e}", exc_info=True)