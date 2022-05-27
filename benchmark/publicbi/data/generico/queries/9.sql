SELECT "Generico_5"."Anunciante" AS "Datos (copia)",   MAX("Generico_5"."Vehiculo") AS "TEMP(attr:Vehiculo:nk)(1662645443)(0)",   MIN("Generico_5"."Vehiculo") AS "TEMP(attr:Vehiculo:nk)(536654816)(0)",   CAST(EXTRACT(MONTH FROM "Generico_5"."FECHA") AS BIGINT) AS "mn:FECHA:ok",   SUM(CAST("Generico_5"."NumAnuncios" AS BIGINT)) AS "sum:NumAnuncios:ok",   CAST(EXTRACT(YEAR FROM "Generico_5"."FECHA") AS BIGINT) AS "yr:FECHA:ok" FROM "Generico_5" WHERE (("Generico_5"."Anunciante" IN ('BANTRAB/TODOTICKET', 'TODOTICKET', 'TODOTICKET.COM')) AND (((CAST(EXTRACT(MONTH FROM "Generico_5"."FECHA") AS BIGINT) >= 2) AND (CAST(EXTRACT(MONTH FROM "Generico_5"."FECHA") AS BIGINT) <= 12)) OR (CAST(EXTRACT(MONTH FROM "Generico_5"."FECHA") AS BIGINT) IS NULL)) AND (CAST(EXTRACT(YEAR FROM "Generico_5"."FECHA") AS BIGINT) = 2015) AND ("Generico_5"."Medio" = 'TELEVISION NACIONAL')) GROUP BY "Generico_5"."Anunciante",   "mn:FECHA:ok",   "yr:FECHA:ok" ORDER BY "Datos (copia)";
