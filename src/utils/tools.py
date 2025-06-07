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
from typing import Optional, Union


# Logging estruturado
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
)
logger = logging.getLogger(__name__)

ELASTIC_INDEX_SUCCESS = "compass_dt_datametrics"
ELASTIC_INDEX_FAIL = "compass_dt_datametrics_fail"

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
            save_metrics(
                metrics_type="fail",
                index=ELASTIC_INDEX_FAIL,
                error=f"Nenhum dado foi carregado para unificar: Volume Google: {countGoogle}, Volume MongoDB: {countMongo}, Volume Apple: {countApple}"
            )

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
        save_metrics(
            metrics_type="fail",
            index=ELASTIC_INDEX_FAIL,
            error=e
        )
        raise




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
        save_metrics(
            metrics_type="fail",
            index=ELASTIC_INDEX_FAIL,
            error=e
        )
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
        save_metrics(
            metrics_type="fail",
            index=ELASTIC_INDEX_FAIL,
            error=e
        )
        raise


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
        save_metrics(
            metrics_type="fail",
            index=ELASTIC_INDEX_FAIL,
            error=e
        )



def save_dataframe(
        df: DataFrame,
        path: str,
        label: str,
        schema: Optional[StructType] = None,
        partition_column: str = "odate",
        compression: str = "snappy"
) -> bool:
    """
    Salva um DataFrame Spark no formato Parquet de forma robusta.

    Args:
        df: DataFrame a ser salvo
        path: Caminho de destino
        label: Identificação para logs (ex: 'valido', 'invalido')
        schema: Schema opcional para validação
        partition_column: Coluna de partição
        compression: Tipo de compressão

    Returns:
        bool: True se salvou com sucesso, False caso contrário

    Raises:
        ValueError: Se os parâmetros forem inválidos
        IOError: Se houver problemas ao escrever no filesystem
    """
    if not isinstance(df, DataFrame):
        logger.error(f"[*] Objeto passado não é um DataFrame Spark: {type(df)}")
        return False

    if not path:
        logger.error("Caminho de destino não pode ser vazio")
        return False

    current_date = datetime.now().strftime('%Y%m%d')
    full_path = Path(path)

    try:
        if schema:
            logger.info(f"[*] Aplicando schema para dados {label}")
            df = get_schema(df, schema)

        df_partition = df.withColumn(partition_column, lit(current_date))

        if not df_partition.head(1):
            logger.warning(f"[*] Nenhum dado {label} encontrado para salvar")
            return False

        try:
            full_path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"[*] Diretório {full_path} verificado/criado")
        except Exception as dir_error:
            logger.error(f"[*] Falha ao preparar diretório {full_path}: {dir_error}")
            save_metrics(
                metrics_type="fail",
                index=ELASTIC_INDEX_FAIL,
                error=e
            )
            raise IOError(f"[*] Erro de diretório: {dir_error}") from dir_error

        logger.info(f"[*] Salvando {df_partition.count()} registros ({label}) em {full_path}")

        (df_partition.write
         .option("compression", compression)
         .mode("overwrite")
         .partitionBy(partition_column)
         .parquet(str(full_path)))

        logger.info(f"[*] Dados {label} salvos com sucesso em {full_path}")
        return True

    except Exception as e:
        error_msg = f"[*] Falha ao salvar dados {label} em {full_path}"
        logger.error(error_msg, exc_info=True)
        logger.error(f"[*] Detalhes do erro: {str(e)}\n{traceback.format_exc()}")
        save_metrics(
            metrics_type="fail",
            index=ELASTIC_INDEX_FAIL,
            error=e
        )
        return False

def save_metrics(
        metrics_type: str,
        index: str,
        error: Optional[Exception] = None,
        df: Optional[DataFrame] = None,
        metrics_data: Optional[Union[dict, str]] = None
) -> None:
    """
    Salva métricas no Elasticsearch com estruturas específicas.

    Args:
        metrics_type: 'success' ou 'fail'
        index: Nome do índice no Elasticsearch
        error: Objeto de exceção (para tipo 'fail')
        df: DataFrame (para extrair segmentos)
        metrics_data: Dados das métricas (para tipo 'success')

    Raises:
        ValueError: Se os parâmetros forem inválidos
    """
    metrics_type = metrics_type.lower()

    if metrics_type not in ('success', 'fail'):
        raise ValueError("[*] O tipo deve ser 'success' ou 'fail'")

    if metrics_type == 'fail' and not error:
        raise ValueError("[*] Para tipo 'fail', o parâmetro 'error' é obrigatório")

    if metrics_type == 'success' and not metrics_data:
        raise ValueError("[*] Para tipo 'success', 'metrics_data' é obrigatório")

    ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
    ES_USER = os.getenv("ES_USER")
    ES_PASS = os.getenv("ES_PASS")

    if not all([ES_USER, ES_PASS]):
        raise ValueError("[*] Credenciais do Elasticsearch não configuradas")

    if metrics_type == 'fail':
        try:
            segmentos_unicos = [row["segmento"] for row in df.select("segmento").distinct().collect()] if df else ["UNKNOWN_CLIENT"]
        except Exception:
            logger.warning("[*] Não foi possível extrair segmentos. Usando 'UNKNOWN_CLIENT'.")
            segmentos_unicos = ["UNKNOWN_CLIENT"]

        document = {
            "timestamp": datetime.now().isoformat(),
            "layer": "gold",
            "project": "compass",
            "job": "aggregate_apps_reviews",
            "priority": "0",
            "tower": "SBBR_COMPASS",
            "client": segmentos_unicos,
            "error": str(error) if error else "Erro desconhecido"
        }
    else:
        if isinstance(metrics_data, str):
            try:
                document = json.loads(metrics_data)
            except json.JSONDecodeError as e:
                raise ValueError("[*] metrics_data não é um JSON válido") from e
        else:
            document = metrics_data

    try:
        es = Elasticsearch(
            hosts=[ES_HOST],
            basic_auth=(ES_USER, ES_PASS),
            request_timeout=30
        )

        response = es.index(
            index=index,
            document=document
        )

        logger.info(f"[*] Métricas salvas com sucesso no índice {index}. ID: {response['_id']}")
        return response

    except Exception as es_error:
        logger.error(f"[*] Falha ao salvar no Elasticsearch: {str(es_error)}")
        raise
    except Exception as e:
        logger.error(f"[*] Erro inesperado: {str(e)}")
        raise