SELECT
  c.nome_cliente
 , SUM(CAST(pe.valor_total AS DOUBLE)) AS valor_pedido
FROM
  tb_aula_sql_cliente AS c
INNER JOIN
  tb_aula_sql_pedido AS pe
WHERE
  pe.data_pedido = '07/10/2021'
GROUP BY
  c.nome_cliente
ORDER BY
  c.nome_cliente DESC;