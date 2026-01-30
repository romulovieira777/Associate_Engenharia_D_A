SELECT
  c.nome_cliente
 , SUM(pe.valor_total) AS valor_pedido
FROM
  tb_aula_sql_cliente AS c
INNER JOIN
  tb_aula_sql_pedido AS pe
WHERE
  c.id_cliente = pe.id_cliente
GROUP BY
  c.nome_cliente
ORDER BY
  c.nome_cliente DESC;

SELECT
  pe.data_pedido
 , ip.quantidade
FROM
  tb_aula_sql_pedido AS pe
INNER JOIN
  tb_aula_sql_item_pedido AS ip
ON
  pe.id_pedido = ip.id_pedido;