SELECT "Motos_1"."Marca" AS "Datos (copia)",   SUM("Motos_1"."InversionUS") AS "TEMP(TC_)(2622528870)(0)",   SUM("Motos_1"."InversionUS") AS "sum:Calculation_0061002123102817:ok",   CAST(EXTRACT(YEAR FROM "Motos_1"."FECHA") AS BIGINT) AS "yr:FECHA:ok" FROM "Motos_1" WHERE ((CAST(EXTRACT(YEAR FROM "Motos_1"."FECHA") AS BIGINT) >= 2010) AND (CAST(EXTRACT(YEAR FROM "Motos_1"."FECHA") AS BIGINT) <= 2015) AND ("Motos_1"."Categoria" = 'MOTOCICLETAS')) GROUP BY "Motos_1"."Marca",   "yr:FECHA:ok" ORDER BY "Datos (copia)";
