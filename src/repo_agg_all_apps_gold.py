import logging
import json
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import (
    col, coalesce, lit, avg, max, min, count, round, when, input_file_name, regexp_extract, upper
)
from datetime import datetime
from pyspark.sql.types import IntegerType, StringType

try:
    from tools import *
    from metrics import MetricsCollector, validate_ingest
except ModuleNotFoundError:
    from src.utils.tools import *
    from src.metrics.metrics import MetricsCollector, validate_ingest


# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():

    # Criação da sessão Spark
    spark = spark_session()

    try:
        # Coleta de métricas
        metrics_collector = MetricsCollector(spark)
        metrics_collector.start_collection()


        path_google_play = "/santander/silver/compass/reviews/googlePlay"
        path_mongodb = "/santander/silver/compass/reviews/mongodb"
        path_apple_store = "/santander/silver/compass/reviews/appleStore"

        df = processing_reviews(path_google_play, path_mongodb, path_apple_store)

        # Validação e separação dos dados
        valid_df, invalid_df, validation_results = validate_ingest(spark, df)

        # Consolidação de colunas equivalentes
        df = valid_df.withColumn("rating", F.col("rating").cast("double")) \
            .withColumn("final_rating", F.coalesce(F.col("rating"), F.lit(None).cast("double"))) \
            .withColumn("final_date", F.coalesce(F.col("iso_date"), F.col("odate"))) \
            .withColumn("final_comment", F.coalesce(F.col("snippet"), F.col("title"))) \
            .withColumn("final_app_version", F.lit(None)) \
            .withColumn(
                "final_name_client",
                F.when(F.col("snippet").isNotNull(), F.col("title"))
                .otherwise(F.lit(None))
            )

        # Seleciona apenas as colunas consolidadas para facilitar o processamento
        df_consolidado = df.select(
            F.col("app").alias("app_nome"),
            F.col("app_source").alias("app_source"),
            F.col("final_rating").alias("rating"),
            F.col("final_date").alias("date"),
            F.col("final_comment").alias("comment"),
            F.col("final_app_version").alias("app_version"),
            F.col("final_name_client").alias("name_client")
        )

        # Converte o app_nome para uppercase
        df_consolidado = df_consolidado.withColumn("app_nome", F.upper(F.col("app_nome")))

        # Converte a data para o formato YYYY-MM
        df_consolidado = df_consolidado.withColumn("periodo_referencia", F.col("date").substr(1, 7))
        df_filtrado = df_consolidado.filter(F.col("periodo_referencia").rlike(r"^\d{4}-\d{2}$"))

        # Regex mais robusta para comentários positivos
        comentarios_positivos = F.count(F.when(
            F.col("comment").rlike(r"(?i)\b(ótimo|excelente|bom)\b(?!.*\b(não|nem|nunca|jamais)\b)"), 1)
            .otherwise(None)).alias("comentarios_positivos")

        # Regex mais robusta para comentários negativos
        comentarios_negativos = F.count(F.when(
            F.col("comment").rlike(r"(?i)\b(ruim|péssimo|horrível)\b(?!.*\b(não|nem|nunca|jamais)\b)"), 1)
            .otherwise(None)).alias("comentarios_negativos")

        # Agregações para a tabela Gold
        gold_df = df_filtrado.groupBy("app_nome", "app_source", "periodo_referencia").agg(
            F.round(F.avg("rating"), 1).alias("nota_media"),
            F.round(
                (F.max("rating") - F.min("rating")) / F.max("rating") * 100, 2
            ).alias("nota_tendencia"),
            F.count("*").alias("avaliacoes_total"),
            comentarios_positivos,
            comentarios_negativos,
        ).withColumn("app_source", upper(col("app_source")))

        # Exibe o resultado
        gold_df.orderBy(col("periodo_referencia").desc(), col("app_nome").desc()).show(gold_df.count(), truncate=False)


        # Coleta de métricas após processamento
        metrics_collector.end_collection()
        metrics_json = metrics_collector.collect_metrics(valid_df, invalid_df, validation_results, "gold_aggregate")

        # Definindo caminhos
        datePath = datetime.now().strftime("%Y%m%d")

        # Salvando dados e métricas
        path_target = f"/santander/gold/compass/reviews/apps_santander_aggregate/odate={datePath}/"
        path_target_fail = f"/santander/gold/compass/reviews_fail/apps_santander_aggregate/odate={datePath}/"


        save_data(spark, valid_df, invalid_df,path_target, path_target_fail)
        save_data_mongo(gold_df.distinct(), "dt_d_view_gold_agg_compass") # salva visao gold no mongo

        # salva visao das avaliacoes no mongo para usuarios e executivos
        df_visao_silver = valid_df.select(
            F.upper("app").alias("app"),
            valid_df["rating"].cast(IntegerType()).alias("rating"),
            # Padronizar o formato da data 'iso_date'
            F.when(
                F.col("iso_date").rlike(r"\.\d{6}$"),  # Caso tenha milissegundos (como no exemplo '2024-11-30T23:10:15.494921')
                F.to_timestamp("iso_date", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
            ).when(
                F.col("iso_date").rlike(r"Z$"),  # Caso seja o formato com 'Z' no final (UTC)
                F.to_timestamp("iso_date", "yyyy-MM-dd'T'HH:mm:ss'Z'")
            ).otherwise(
                F.to_timestamp("iso_date", "yyyy-MM-dd'T'HH:mm:ssZ")  # Caso tenha o fuso horário
            ).alias("iso_date"),

            # Usando date_format para garantir que todas as datas tenham 3 casas decimais de milissegundos
            F.date_format(
                F.when(
                    F.col("iso_date").isNotNull(),
                    F.col("iso_date")
                ).otherwise(F.lit(None)),
                "yyyy-MM-dd HH:mm:ss.SSS"
            ).alias("iso_date")
        )

        save_data_mongo(df_visao_silver.distinct(), "dt_d_view_silver_historical_compass")
        save_metrics(metrics_json)

    except Exception as e:
        logging.error(f"[*] An error occurred: {e}", exc_info=True)
        # JSON de erro
        error_metrics = {
            "data_e_hora": datetime.now().isoformat(),
            "camada": "gold",
            "grupo": "compass",
            "job": "aggregate_apps_reviews",
            "relevancia": "0",
            "torre": "SBBR_COMPASS",
            "erro": str(e)
        }

        metrics_json = json.dumps(error_metrics)

        # Salvar métricas de erro no MongoDB
        save_metrics_job_fail(metrics_json)
        

def spark_session():
    """
    Cria e retorna uma sessão Spark.
    """
    try:
        spark = SparkSession.builder \
            .appName("App Reviews [aggregate]") \
            .config("spark.jars.packages", "org.apache.spark:spark-measure_2.12:0.16") \
            .config("spark.sql.parquet.enableVectorizedReader", "false") \
            .getOrCreate()
        return spark
    except Exception as e:
        logging.error(f"[*] Failed to create SparkSession: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()

