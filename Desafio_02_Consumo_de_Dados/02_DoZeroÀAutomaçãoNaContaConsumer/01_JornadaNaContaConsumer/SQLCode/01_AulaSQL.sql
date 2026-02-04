DROP TABLE IF EXISTS tb_onboarding_estados;

CREATE EXTERNAL TABLE IF NOT EXISTS tb_onboarding_estados (
  cod_estado STRING
 , nome_estado STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://datalake-i-analytics-raw-zone/consumer/onboarding/estados/'
TBLPROPERTIES ('skip.header.line.count'='1');

SELECT
  *
FROM
  tb_onboarding_estados;

SELECT
  banco
 , SUBSTR(CAST(anomesdia AS VARCHAR)) AS anomes
 , MAX(anomesdia) AS maiordata
FROM
  base_de_dados
WHERE
  anomesdia >= 20230101 AND banco = codigo  
GROUP BY
  banco
 , SUBSTR(CAST(anomesdia AS VARCHAR))
ORDER BY
  SUBSTR(CAST(anomesdia AS VARCHAR)) DESC;

SELECT
  SUBSTR(CAST(ags.anomesdia AS VARCHAR(8)), 1, 6) AS anomes
 , ags.anomesdia                  AS datames
 , ags.bairro
 , ags.cidade
 , ags.estado                   AS estado_sigla
 , COUNT(*)                    AS quantidade_agencias
FROM
  base_de_dados AS ags
 , (SELECT banco, SUBSTR(CAST(ags.anomesdia AS VARCHAR(8)), 1, 6) AS anomes, MAX(anomesdia) AS maiordata
  FROM base_de_dados
  WHERE anomesdia >= 20230101 AND banco = codigo
  GROUP BY banco, SUBSTR(CAST(anomesdia AS VARCHAR))
  ORDER BY SUBSTR(CAST(anomesdia AS VARCHAR) DESC
  ) AS dtas
WHERE
  ags.anomesdia = dtas.maiordata AND ags.banco = dtas.banco
GROUP BY
  SUBSTR(CAST(ags.anomesdia AS VARCHAR(8)), 1, 6)
 , ags.anomesdia
 , ags.bairro
 , ags.cidade
 , ags.estado
ORDER BY
  SUBSTR(CAST(ags.anomesdia AS VARCHAR(8)), 1, 6) DESC
LIMIT 10;

DROP TABLE IF EXISTS tb_onboarding_estados;

CREATE EXTERNAL TABLE IF NOT EXISTS tb_onboarding_agencias_tratadas (
  anomes       STRING
 , datames       STRING
 , bairro       STRING
 , cidade       STRING
 , estado_sigla    STRING
 , quantidade_agencias INT
)
LOCATION 's3://datalake-i-analytics-processed-zone/consumer/onboarding/agencias_tratadas/'
TBLPROPERTIES ('table_type'='ICEBERG');

INSERT INTO tb_onboarding_agencias_tratadas
SELECT
  SUBSTR(CAST(ags.anomesdia AS VARCHAR(8)), 1, 6) AS anomes
 , ags.anomesdia                  AS datames
 , ags.bairro
 , ags.cidade
 , ags.estado                   AS estado_sigla
 , COUNT(*)                    AS quantidade_agencias
FROM
  base_de_dados AS ags
 , (SELECT banco, SUBSTR(CAST(ags.anomesdia AS VARCHAR(8)), 1, 6) AS anomes, MAX(anomesdia) AS maiordata
  FROM base_de_dados
  WHERE anomesdia >= 20230101
  GROUP BY banco, SUBSTR(CAST(anomesdia AS VARCHAR))
  ) AS dtas
WHERE
  ags.anomesdia = dtas.maiordata AND ags.banco = dtas.banco
GROUP BY
  SUBSTR(CAST(ags.anomesdia AS VARCHAR(8)), 1, 6)
 , ags.anomesdia
 , ags.bairro
 , ags.cidade
 , ags.estado;

SELECT
  *
FROM
  tb_onboarding_agencias_tratadas;