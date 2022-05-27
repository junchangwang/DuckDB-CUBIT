SELECT COUNT(DISTINCT "MulheresMil_1"."Calculation_838513981462429699") AS "ctd:Calculation_838513981462429699:ok",   CAST(EXTRACT(YEAR FROM "MulheresMil_1"."data_de_inicio") AS BIGINT) AS "yr:data_de_inicio:ok" FROM "MulheresMil_1" WHERE ((CAST(EXTRACT(YEAR FROM "MulheresMil_1"."data_de_inicio") AS BIGINT) = 2015) AND (NOT (("MulheresMil_1"."nome da sit matricula (situacao detalhada)" IN ('CANC_DESISTENTE', 'CANC_MAT_PRIM_OPCAO', 'CANC_SANÇÃO', 'CANC_SEM_FREQ_INICIAL', 'CANC_TURMA', 'DOC_INSUFIC', 'ESCOL_INSUFIC', 'INC _ITINERARIO', 'INSC_CANC', 'Não Matriculado', 'NÃO_COMPARECEU', 'TURMA_CANC', 'VAGAS_INSUFIC')) OR ("MulheresMil_1"."nome da sit matricula (situacao detalhada)" IS NULL))) AND (NOT ("MulheresMil_1"."situacao_da_turma" IN ('CANCELADA', 'CRIADA', 'PUBLICADA')))) GROUP BY "yr:data_de_inicio:ok" ORDER BY "ctd:Calculation_838513981462429699:ok";
