SELECT COUNT(DISTINCT "Uberlandia_1"."Calculation_838513981462429699") AS "ctd:Calculation_838513981462429699:ok",   "Uberlandia_1"."ead" AS "ead",   "Uberlandia_1"."edital" AS "edital",   "Uberlandia_1"."forma_ingresso" AS "forma_ingresso",   "Uberlandia_1"."no_modalidade" AS "no_modalidade" FROM "Uberlandia_1" WHERE ((NOT (("Uberlandia_1"."nome da sit matricula (situacao detalhada)" IN ('CANC_DESISTENTE', 'CANC_MAT_PRIM_OPCAO', 'CANC_SANÇÃO', 'CANC_SEM_FREQ_INICIAL', 'CANC_TURMA', 'DOC_INSUFIC', 'ESCOL_INSUFIC', 'INC _ITINERARIO', 'INSC_CANC', 'Não Matriculado', 'NÃO_COMPARECEU', 'TURMA_CANC', 'VAGAS_INSUFIC')) OR ("Uberlandia_1"."nome da sit matricula (situacao detalhada)" IS NULL))) AND (NOT ("Uberlandia_1"."situacao_da_turma" IN ('CANCELADA', 'CRIADA', 'PUBLICADA'))) AND (CAST(EXTRACT(YEAR FROM "Uberlandia_1"."data_de_inicio") AS BIGINT) = 2015)) GROUP BY "ead",   "edital",   "forma_ingresso",   "no_modalidade" ORDER BY "ctd:Calculation_838513981462429699:ok";
