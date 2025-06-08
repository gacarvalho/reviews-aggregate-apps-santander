🧭 ♨️ COMPASS
---

<p align="left">
  <img src="https://img.shields.io/badge/projeto-Compass-blue?style=flat-square" alt="Projeto">
  <img src="https://img.shields.io/badge/versão aplicação-1.0.1-blue?style=flat-square" alt="Versão Aplicação">
  <img src="https://img.shields.io/badge/status-deployed-green?style=flat-square" alt="Status">
  <img src="https://img.shields.io/badge/autor-Gabriel_Carvalho-lightgrey?style=flat-square" alt="Autor">
</p>

Essa aplicação faz parte do projeto **compass-deployment** que é uma solução desenvolvida no contexto do programa Data Master, promovido pela F1rst Tecnologia, com o objetivo de disponibilizar uma plataforma robusta e escalável para captura, processamento e análise de feedbacks de clientes do Banco Santander.


![<data-master-compass>](https://github.com/gacarvalho/repo-spark-delta-iceberg/blob/main/header.png?raw=true)



`📦 artefato` `iamgacarvalho/dmc-reviews-aggregate-apps-santander`

- **Versão:** `1.0.1`
- **Repositório:** [GitHub](https://github.com/gacarvalho/reviews-aggregate-apps-santander)
- **Imagem Docker:** [Docker Hub](https://hub.docker.com/repository/docker/iamgacarvalho/dmc-reviews-aggregate-apps-santander/tags/1.0.1/sha256-58173fc5e2bc379e19dc5496c1da79f1ccaac0535a5ab5ae27430f64050f98ac)
- **Descrição:**  Coleta avaliações de clientes de diversos canais ingeridos no Data Lake, realizando a ingestão a partir da camada Silver, processando, agregando as informações e armazenando no HDFS em formato **Parquet**.
- **Parâmetros:**


    - `$CONFIG_ENV` (`Pre`, `Pro`) → Define o ambiente: `Pre` (Pré-Produção), `Pro` (Produção).

| Componente          | Descrição                                                                                                                               |
|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| **Objetivo**        | Processar avaliações de clientes de diversos canais (camada Silver → Gold), garantindo: agregação dos dados conforme regras de negócio. |
| **Entrada**         | Ambiente (pre/prod)                                                                                                                     |
| **Saída**           | Dados válidos/inválidos em Parquet + métricas no Elasticsearch                                                                          |
| **Tecnologias**     | PySpark, Elasticsearch, Parquet, SparkMeasure                                                                                           |
| **Fluxo Principal** | 1. Coleta dos dados Silver → 2. Aplica Agregação → 3. Armazenamento HDFS e MongoDB                                                      |
| **Validações**      | Duplicatas, nulos em campos críticos, consistência de tipos                                                                             |
| **Particionamento** | Por data referencia de carga (odate)                                                                                                    |
| **Métricas**        | Tempo execução, memória, registros válidos/inválidos, performance Spark                                                                 |
| **Tratamento Erros**| Logs detalhados, armazenamento separado de dados inválidos                                                                              |
| **Execução**        | `repo_agg_all_apps_gold.py <env>`                                                                                                       |
