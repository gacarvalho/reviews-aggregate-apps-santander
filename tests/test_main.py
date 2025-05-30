import os
import pytest
import shutil
import tempfile
import pyspark.sql.functions as F
from datetime import datetime
from pyspark.sql.functions import (
    col, coalesce, lit, avg, max, min, count, round, when, input_file_name, regexp_extract, upper
)
from pyspark.sql.types import IntegerType, StringType,StructType
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from src.utils.tools import processing_reviews, save_data_mongo, save_data
from src.metrics.metrics import validate_ingest

@pytest.fixture
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("Unit Test") \
        .getOrCreate()

@pytest.fixture
def setup_test_data_google():
    # Caminho relativo ao diretório do projeto
    base_path = os.path.dirname(__file__)
    sample_file = os.path.join(
        base_path,
        "source/santander/silver/compass/reviews/googlePlay/odate=20250510/part-00000-4b8d2e30-9ebe-483f-8e40-3bdf2876964d-c000.snappy.parquet",
    )
    # Verifica se o arquivo existe antes de copiar
    if not os.path.exists(sample_file):
        raise FileNotFoundError(f"Arquivo não encontrado: {sample_file}")

    # Cria diretório temporário
    temp_dir = tempfile.mkdtemp()
    shutil.copy(sample_file, temp_dir)
    yield temp_dir
    shutil.rmtree(temp_dir)

@pytest.fixture
def setup_test_data_apple():
    # Caminho relativo ao diretório do projeto
    base_path = os.path.dirname(__file__)
    sample_file = os.path.join(
        base_path,
        "source/santander/silver/compass/reviews/appleStore/odate=20250510/part-00000-c63468f0-1cf8-4cb6-b6fb-68a99364d45a-c000.snappy.parquet",
    )
    # Verifica se o arquivo existe antes de copiar
    if not os.path.exists(sample_file):
        raise FileNotFoundError(f"Arquivo não encontrado: {sample_file}")

    # Cria diretório temporário
    temp_dir = tempfile.mkdtemp()
    shutil.copy(sample_file, temp_dir)
    yield temp_dir
    shutil.rmtree(temp_dir)

@pytest.fixture
def setup_test_data_mongo():
    # Caminho relativo ao diretório do projeto
    base_path = os.path.dirname(__file__)
    sample_file = os.path.join(
        base_path,
        "source/santander/silver/compass/reviews/mongodb/odate=20250510/part-00000-b8eb64b5-6b3b-42ed-8afe-96172e291471-c000.snappy.parquet",
    )
    # Verifica se o arquivo existe antes de copiar
    if not os.path.exists(sample_file):
        raise FileNotFoundError(f"Arquivo não encontrado: {sample_file}")

    # Cria diretório temporário
    temp_dir = tempfile.mkdtemp()
    shutil.copy(sample_file, temp_dir)
    yield temp_dir
    shutil.rmtree(temp_dir)

def test_read_data(spark, setup_test_data_google, setup_test_data_apple, setup_test_data_mongo):
    # Caminhos temporários para leitura
    temp_path_google = f"{setup_test_data_google}/*.parquet"
    temp_path_apple = f"{setup_test_data_apple}/*.parquet"
    temp_path_mongodb = f"{setup_test_data_mongo}/*.parquet"

    df_google = spark.read.parquet(temp_path_google)
    df_apple = spark.read.parquet(temp_path_apple)
    df_mongodb = spark.read.parquet(temp_path_mongodb)

    assert df_google.count() > 0
    assert df_apple.count() > 0
    assert df_mongodb.count() > 0

def test_processamento_reviews(spark, setup_test_data_google, setup_test_data_apple, setup_test_data_mongo):
    # Caminhos temporários para leitura
    temp_path_google = f"{setup_test_data_google}/*.parquet"
    temp_path_apple = f"{setup_test_data_apple}/*.parquet"
    temp_path_mongodb = f"{setup_test_data_mongo}/*.parquet"

    df1 = spark.read.parquet(temp_path_google)
    df2 = spark.read.parquet(temp_path_apple)
    df3 = spark.read.parquet(temp_path_mongodb)

    # Caminhos temporários para escrita
    path_google = tempfile.mkdtemp()
    path_mongodb = tempfile.mkdtemp()
    path_apple = tempfile.mkdtemp()

    df1.write.mode("overwrite").parquet(path_google)
    df2.write.mode("overwrite").parquet(path_mongodb)
    df3.write.mode("overwrite").parquet(path_apple)

    # Teste da função de processamento
    df = processing_reviews(spark, f"{path_google}/*.parquet", f"{path_apple}/*.parquet", f"{path_mongodb}/*.parquet")

    assert df.count() > 0
    # Verifique se o número de registros no DataFrame é o esperado
    assert df.count() == 7974, f"Esperado 7974 registros, mas encontrou {df.count()}."

def test_validate_ingest(spark, setup_test_data_google, setup_test_data_apple, setup_test_data_mongo):
    """
    Testa a função de validação de ingestão para garantir que os DataFrames têm dados e que a validação gera resultados.
    """
    # Caminhos temporários para leitura
    temp_path_google = f"{setup_test_data_google}/*.parquet"
    temp_path_apple = f"{setup_test_data_apple}/*.parquet"
    temp_path_mongodb = f"{setup_test_data_mongo}/*.parquet"

    df1 = spark.read.parquet(temp_path_google)
    df2 = spark.read.parquet(temp_path_apple)
    df3 = spark.read.parquet(temp_path_mongodb)

    # Caminhos temporários para escrita
    path_google = tempfile.mkdtemp()
    path_mongodb = tempfile.mkdtemp()
    path_apple = tempfile.mkdtemp()

    df1.write.mode("overwrite").parquet(path_google)
    df2.write.mode("overwrite").parquet(path_mongodb)
    df3.write.mode("overwrite").parquet(path_apple)
    df3.write.mode("append").parquet(path_apple)

    # Teste da função de processamento
    df = processing_reviews(spark, f"{path_google}/*.parquet", f"{path_apple}/*.parquet", f"{path_mongodb}/*.parquet")


    # Valida o DataFrame e coleta resultados
    valid_df, invalid_df, validation_results = validate_ingest(df)

    assert valid_df.count() > 0, "[*] O DataFrame válido está vazio!"
    assert invalid_df.count() > 0, "[*] O DataFrame inválido está vazio!"
    assert len(validation_results) > 0, "[*] Não foram encontrados resultados de validação!"

    # Opcional: Exibir resultados para depuração
    print("Testes realizados com sucesso!")
    print(f"Total de registros válidos: {valid_df.count()}")
    print(f"Total de registros inválidos: {invalid_df.count()}")
    print(f"Resultados da validação: {validation_results}")

def test_save_data(spark, setup_test_data_google, setup_test_data_apple, setup_test_data_mongo):
    # Caminhos temporários para leitura
    temp_path_google = f"{setup_test_data_google}/*.parquet"
    temp_path_apple = f"{setup_test_data_apple}/*.parquet"
    temp_path_mongodb = f"{setup_test_data_mongo}/*.parquet"

    df1 = spark.read.parquet(temp_path_google)
    df2 = spark.read.parquet(temp_path_apple)
    df3 = spark.read.parquet(temp_path_mongodb)

    # Caminhos temporários para escrita
    path_google = tempfile.mkdtemp()
    path_mongodb = tempfile.mkdtemp()
    path_apple = tempfile.mkdtemp()

    df1.write.mode("overwrite").parquet(path_google)
    df2.write.mode("overwrite").parquet(path_mongodb)
    df3.write.mode("overwrite").parquet(path_apple)
    df3.write.mode("append").parquet(path_apple)

    # Teste da função de processamento
    df = processing_reviews(spark, f"{path_google}/*.parquet", f"{path_apple}/*.parquet", f"{path_mongodb}/*.parquet")

    # Valida o DataFrame e coleta resultados
    valid_df, invalid_df, validation_results = validate_ingest(df)

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

    # Seleciona apenas as colunas consolidadas
    df_consolidado = df.select(
        F.col("app").alias("app_nome"),
        F.col("app_source").alias("app_source"),
        F.col("segmento").alias("segmento"),
        F.col("final_rating").alias("rating"),
        F.col("final_date").alias("date"),
        F.col("final_comment").alias("comment"),
        F.col("final_app_version").alias("app_version"),
        F.col("final_name_client").alias("name_client")
    )

    df_consolidado = df_consolidado.withColumn("app_nome", F.upper(F.col("app_nome")))
    df_consolidado = df_consolidado.withColumn("periodo_referencia", F.col("date").substr(1, 7))
    df_filtrado = df_consolidado.filter(F.col("periodo_referencia").rlike(r"^\d{4}-\d{2}$"))

    # Comentários positivos
    comentarios_positivos = F.count(F.when(
        F.col("comment").rlike(r"(?i)\b(ótimo|excelente|bom)\b(?!.*\b(não|nem|nunca|jamais)\b)"), 1)
    ).alias("comentarios_positivos")

    # Comentários negativos
    comentarios_negativos = F.count(F.when(
        F.col("comment").rlike(r"(?i)\b(ruim|péssimo|horrível)\b(?!.*\b(não|nem|nunca|jamais)\b)"), 1)
    ).alias("comentarios_negativos")

    gold_df = df_filtrado.groupBy("app_nome", "app_source", "periodo_referencia", F.upper("segmento").alias("segmento")).agg(
        F.round(F.avg("rating"), 1).alias("nota_media"),
        F.round((F.max("rating") - F.min("rating")) / F.max("rating") * 100, 2).alias("nota_tendencia"),
        F.count("*").alias("avaliacoes_total"),
        comentarios_positivos,
        comentarios_negativos
    )

    gold_df.orderBy(col("periodo_referencia").desc(), col("app_nome").desc()).show(gold_df.count(), truncate=False)

    # Caminhos de escrita
    datePath = datetime.now().strftime("%Y%m%d")
    path_target = f"/tmp/fake/path/valid/"
    path_target_fail = f"/tmp/fake/path/invalid/odate={datePath}/"

    # Patch das variáveis de ambiente com ES_PASS corrigido
    with patch.dict(os.environ, {"ES_USER": "fake", "ES_PASS": "fake", "ES_HOST": "localhost"}):

        # Mock do método de escrita parquet
        with patch("pyspark.sql.DataFrameWriter.parquet", MagicMock()) as mock_parquet:

            # Salvar dados
            save_data(spark, valid_df, invalid_df, path_target, path_target_fail)
            save_data_mongo(gold_df, "dt_d_view_gold_agg_compass")

            # salva visao das avaliacoes no mongo para usuarios e executivos
            df_visao_silver = valid_df.select(
                upper("title").alias("title"),
                upper("snippet").alias("snippet"),
                upper("app_source").alias("app_source"),
                upper("app").alias("app"),
                F.col("segmento").alias("segmento"),
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
                ).alias("iso_date")
            )
            save_data_mongo(df_visao_silver, "dt_d_view_silver_historical_compass")

            # Verificações
            mock_parquet.assert_any_call(path_target)
            mock_parquet.assert_any_call(path_target_fail)

        print("[*] Teste de salvar dados concluído com sucesso!")