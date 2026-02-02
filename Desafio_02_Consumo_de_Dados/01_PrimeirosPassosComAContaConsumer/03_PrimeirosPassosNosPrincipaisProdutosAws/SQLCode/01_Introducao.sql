CREATE EXTERNAL TABLE IF NOT EXISTS (
  cod_estado STRING
 , nome_estado STRING
)
LOCATION 's3://squad-c-d-h/03_PrimeirosPassosNosPrincipaisProdutosAws/01_S3/01_Athena/01_Tabela_Estados/base_estados.csv'
FILE FORMAT PARQUET;

SELECT
  *
FROM
  tabela_estados
WHERE
  cod_estado = 'SP';