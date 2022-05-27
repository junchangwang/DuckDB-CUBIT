SELECT "Eixo_1"."no_modalidade" AS "no_modalidade",   "Eixo_1"."nome da sit matricula (situacao detalhada)" AS "nome da sit matricula (situacao detalhada)",   SUM(CAST("Eixo_1"."Number of Records" AS BIGINT)) AS "sum:Number of Records:ok",   CAST(EXTRACT(YEAR FROM "Eixo_1"."data_de_inicio") AS BIGINT) AS "yr:data_de_inicio:ok" FROM "Eixo_1" WHERE (("Eixo_1"."no_modalidade" IN ('pronatec turismo cidadão', 'Pronatec turismo Desenvolvimento Local', 'PRONATEC TURISMO na EMPRESA', 'PRONATEC TURISMO SOCIAL')) AND (NOT (("Eixo_1"."nome da sit matricula (situacao detalhada)" NOT IN ('', 'TRANSF_EXT', 'INTEGRALIZADA', 'FREQ_INIC_INSUF', 'TRANCADA', 'CONCLUÍDA', 'TRANSF_INT', 'EM_CURSO', 'REPROVADA', 'ABANDONO', 'CONFIRMADA', 'EM_DEPENDÊNCIA')) OR ("Eixo_1"."nome da sit matricula (situacao detalhada)" IS NULL))) AND ((CAST(EXTRACT(YEAR FROM "Eixo_1"."data_de_inicio") AS BIGINT) IS NULL) OR ((CAST(EXTRACT(YEAR FROM "Eixo_1"."data_de_inicio") AS BIGINT) >= 1201) AND (CAST(EXTRACT(YEAR FROM "Eixo_1"."data_de_inicio") AS BIGINT) <= 3013))) AND (NOT ("Eixo_1"."situacao_da_turma" IN ('CANCELADA', 'CRIADA', 'PUBLICADA')))) GROUP BY "Eixo_1"."no_modalidade",   "Eixo_1"."nome da sit matricula (situacao detalhada)", "yr:data_de_inicio:ok" ORDER BY "no_modalidade";
