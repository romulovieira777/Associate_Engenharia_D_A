CREATE TABLE base_de_dados.tabela
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
) AS
SELECT
  CASE
    WHEN TRIM(a.dtpprcab) = '' THEN 'Em dia'
    WHEn TRIM(a.dtpprcab) <> '' THEN 'Atrasado'
    END AS Status
 , a.*
FROM
  base_de_dados.tabela
WHERE
  anomesdia = 20230630
AND
  MOD(CAST(numctror AS BIGINT), 20) = 7
AND
  pes = 'F'
LIMIT 1000;
 
SELECT
  COUNT(*) AS total_registros
FROM
  base_de_dados.tabela;