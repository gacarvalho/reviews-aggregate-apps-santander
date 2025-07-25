import os
import sys
import json
import logging
from datetime import datetime
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import (
    col, coalesce, lit, avg, count, when, upper
)
from pyspark.sql.types import IntegerType, StructType

try:
    from tools import *
    from metrics import MetricsCollector, validate_ingest
except ModuleNotFoundError:
    from src.utils.tools import *
    from src.metrics.metrics import MetricsCollector, validate_ingest

# Configuração centralizada
FORMAT = "parquet"
PARTITION_COLUMN = "odate"
COMPRESSION_TYPE = "snappy"
ENV_PRE_VALUE = "pre"
ELASTIC_INDEX_SUCCESS = "compass_dt_datametrics"
ELASTIC_INDEX_FAIL = "compass_dt_datametrics_fail"

PATH_GOOGLE_PLAY = "/santander/silver/compass/reviews/googlePlay"
PATH_MONGODB = "/santander/silver/compass/reviews/mongodb"
PATH_APPLE_STORE = "/santander/silver/compass/reviews/appleStore"
PATH_GOLD_BASE = "/santander/gold/compass/reviews/apps_santander_aggregate/"
PATH_GOLD_FAIL_BASE = "/santander/gold/compass/reviews_fail/apps_santander_aggregate/"

# Constantes de regex
PERIODO_REGEX = r"^\\d{4}-\\d{2}$"
REGEX_POSITIVO = r"(?i)\\b(ótimo|excelente|bom)\\b(?!.*\\b(não|nem|nunca|jamais)\\b)"
REGEX_NEGATIVO = r"(?i)\\b(ruim|péssimo|horrível)\\b(?!.*\\b(não|nem|nunca|jamais)\\b)"

# Logging estruturado
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
)
logger = logging.getLogger(__name__)

class PipelineConfig:
    def __init__(self, env: str):
        self.env = env
        self.date_str = datetime.now().strftime("%Y%m%d")
        self.path_google= f"{PATH_GOOGLE_PLAY}/odate={self.date_str}/"
        self.path_apple= f"{PATH_APPLE_STORE}/odate={self.date_str}/"
        self.path_internaldatabase= f"{PATH_MONGODB}/odate={self.date_str}/"
        self.path_target = f"{PATH_GOLD_BASE}odate={self.date_str}/"
        self.path_target_fail = f"{PATH_GOLD_FAIL_BASE}odate={self.date_str}/"

def create_spark_session() -> SparkSession:
    try:
        spark = SparkSession.builder \
            .appName("App Reviews [aggregate]") \
            .config("spark.jars.packages", "org.apache.spark:spark-measure_2.12:0.16") \
            .config("spark.sql.parquet.enableVectorizedReader", "false") \
            .getOrCreate()
        logger.info("[*] Spark Session criada com sucesso.")
        return spark
    except Exception as e:
        logger.error("[*] Falha ao criar SparkSession", exc_info=True)
        save_metrics(
            metrics_type="fail",
            index=ELASTIC_INDEX_FAIL,
            error=e
        )
        raise

def process_reviews_columns(df: DataFrame) -> DataFrame:
    return df.withColumn("rating", col("rating").cast("double")) \
        .withColumn("final_rating", coalesce(col("rating"), lit(None).cast("double"))) \
        .withColumn("final_date", col("iso_date")) \
        .withColumn("final_comment", coalesce(col("snippet"), col("title"))) \
        .withColumn("final_app_version", lit(None)) \
        .withColumn("final_name_client", when(col("snippet").isNotNull(), col("title")).otherwise(lit(None)))

def build_consolidated_df(df: DataFrame) -> DataFrame:
    return df.select(
        upper(col("app")).alias("app_nome"),
        upper(col("app_source")).alias("app_source"),
        upper(col("segmento")).alias("segmento"),
        col("final_rating").alias("rating"),
        col("final_date").alias("date"),
        col("final_comment").alias("comment"),
        col("final_app_version").alias("app_version"),
        col("final_name_client").alias("name_client")
    ).withColumn("periodo_referencia", col("date").substr(1, 7))

def create_silver_view(df: DataFrame) -> DataFrame:
    return df.select(
        upper("title").alias("title"),
        upper("snippet").alias("snippet"),
        upper("app_source").alias("app_source"),
        upper("app").alias("app"),
        col("segmento"),
        col("rating").cast(IntegerType()).alias("rating"),
        when(
            col("iso_date").rlike(r"\\.\\d{6}$"),
            F.to_timestamp("iso_date", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
        ).when(
            col("iso_date").rlike(r"Z$"),
            F.to_timestamp("iso_date", "yyyy-MM-dd'T'HH:mm:ss'Z'")
        ).otherwise(
            F.to_timestamp("iso_date", "yyyy-MM-dd'T'HH:mm:ssZ")
        ).alias("iso_date")
    )

def execute_pipeline(spark: SparkSession, env: str) -> None:
    try:
        config = PipelineConfig(env)
        metrics_collector = MetricsCollector(spark)
        metrics_collector.start_collection()

        df = processing_reviews(spark, PATH_GOOGLE_PLAY, PATH_MONGODB, PATH_APPLE_STORE)
        valid_df, invalid_df, validation_results = validate_ingest(df)

        df = process_reviews_columns(valid_df)
        df_filtrado = build_consolidated_df(df)

        comentarios_positivos = count(when(col("comment").rlike(REGEX_POSITIVO), 1)).alias("comentarios_positivos")
        comentarios_negativos = count(when(col("comment").rlike(REGEX_NEGATIVO), 1)).alias("comentarios_negativos")

        gold_df = df_filtrado.groupBy("app_nome", "app_source", "periodo_referencia", "segmento").agg(
            F.round(avg("rating"), 1).alias("nota_media"),
            count("*").alias("avaliacoes_total"),
            comentarios_positivos,
            comentarios_negativos
        ).orderBy(col("periodo_referencia").desc(), col("app_nome").desc())

        if env == ENV_PRE_VALUE:
            gold_df.orderBy(col("periodo_referencia").desc(), col("app_nome").desc()).show(gold_df.count(), truncate=False)

        metrics_collector.end_collection()
        metrics_json = metrics_collector.collect_metrics(valid_df, invalid_df, validation_results, "gold_aggregate")

        logger.info(f"[*] Salvando dados válidos em {config.path_target}")
        save_dataframe(
            df=gold_df,
            path=config.path_target,
            label="valido",
            partition_column=PARTITION_COLUMN,
            compression=COMPRESSION_TYPE
        )

        logger.info(f"[*] Salvando dados inválidos em {config.path_target_fail}")
        save_dataframe(
            df=invalid_df,
            path=config.path_target_fail,
            label="invalido",
            partition_column=PARTITION_COLUMN,
            compression=COMPRESSION_TYPE
        )

        save_data_mongo(gold_df, "dt_d_view_gold_agg_compass")

        df_visao_silver = create_silver_view(valid_df)
        save_data_mongo(df_visao_silver.distinct(), "dt_d_view_silver_historical_compass")

        save_metrics(
            metrics_type='success',
            index=ELASTIC_INDEX_SUCCESS,
            df=valid_df,
            metrics_data=metrics_json
        )

        logger.info("[*] Pipeline executado com sucesso.")
    except Exception as e:
        logger.error("[*] Erro na execução do pipeline", exc_info=True)
        save_metrics(
            metrics_type="fail",
            index=ELASTIC_INDEX_FAIL,
            error=e
        )
        log_error(e, spark.createDataFrame([], StructType([])), 0)
        raise

def main():
    if len(sys.argv) != 2:
        logger.error("Uso: spark-submit app.py <env>")
        sys.exit(1)

    env = sys.argv[1]
    spark = None

    try:
        spark = create_spark_session()
        execute_pipeline(spark, env)
    except Exception as e:
        logger.error("[*] Pipeline falhou de forma crítica", exc_info=True)
        save_metrics(
            metrics_type="fail",
            index=ELASTIC_INDEX_FAIL,
            error=e
        )
        sys.exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()