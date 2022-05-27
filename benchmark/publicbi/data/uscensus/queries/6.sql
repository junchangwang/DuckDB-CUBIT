SELECT CAST("USCensus_3"."ESR" AS BIGINT) AS "ESR",   CAST("USCensus_3"."SCH" AS TEXT) AS "SCH",   SUM(CAST("USCensus_3"."Number of Records" AS BIGINT)) AS "sum:Number of Records:ok" FROM "USCensus_3" WHERE (CAST("USCensus_3"."AGEP" AS BIGINT) > 15) GROUP BY "USCensus_3"."ESR",   "SCH",   "USCensus_3"."ESR" ORDER BY "ESR";
