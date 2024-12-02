import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import (
    col, coalesce, lit, avg, max, min, count, round, when, input_file_name, regexp_extract
)
from metrics import MetricsCollector, validate_ingest
from tools import *


# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    try:
        # Criação da sessão Spark
        with spark_session() as spark:

            # Coleta de métricas
            metrics_collector = MetricsCollector(spark)
            metrics_collector.start_collection()

            df = processing_reviews()

            # Validação e separação dos dados
            valid_df, invalid_df, validation_results = validate_ingest(spark, df)

            print("valid_df")
            valid_df.show(truncate=False)

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
            )

            # Exibe o resultado
            gold_df.orderBy(col("periodo_referencia").desc(), col("app_nome").desc()).show(gold_df.count(), truncate=False)

            
            # Coleta de métricas após processamento
            metrics_collector.end_collection()
            metrics_json = metrics_collector.collect_metrics(valid_df, invalid_df, validation_results, "gold_aggregate")

            # Salvando dados e métricas
            save_data(valid_df, invalid_df)
            save_data_gold(gold_df, "dt_d_view_gold_agg_compass") # salva visao gold no mongo
            # salva visao das avaliacoes no mongo para usuarios e executivos
            df_visao_silver = valid_df.select("app","rating","iso_date","title","snippet","app_source")
            save_data_gold(df_visao_silver, "dt_d_view_silver_historical_compass")            
            save_metrics(metrics_json)

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        

def spark_session():
    """
    Cria e retorna uma sessão Spark.
    """
    try:
        spark = SparkSession.builder \
            .appName("App Reviews [google play]") \
            .config("spark.jars.packages", "org.apache.spark:spark-measure_2.12:0.16") \
            .config("spark.sql.parquet.enableVectorizedReader", "false") \
            .getOrCreate()
        return spark
    except Exception as e:
        logging.error(f"Failed to create SparkSession: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()

