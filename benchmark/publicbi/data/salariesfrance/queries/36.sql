SELECT SUM(ABS("SalariesFrance_8"."EMBAUCHE_CDI")) AS "TEMP(Calculation_369576668387979265)(1183172592)(0)",   SUM(ABS("SalariesFrance_8"."EMBAUCHE_CDI")) AS "TEMP(Calculation_369576668387979265)(1291741352)(0)",   SUM(ABS("SalariesFrance_8"."EMBAUCHE_CDD")) AS "TEMP(Calculation_369576668387979265)(3218284358)(0)",   SUM(ABS("SalariesFrance_8"."INTERIM_NP1")) AS "TEMP(Calculation_369576668387979265)(4177782722)(0)",   SUM(ABS("SalariesFrance_8"."BMO_DIFFICILE")) AS "TEMP(Calculation_369576668391063555)(1826962959)(0)",   SUM(ABS("SalariesFrance_8"."BMO_INTENTIONS")) AS "TEMP(Calculation_369576668391063555)(2306732790)(0)",   SUM("SalariesFrance_8"."BMO_INTENTIONS") AS "TEMP(Calculation_369576668391063555)(4168827534)(0)",   SUM("SalariesFrance_8"."EMPSAL_NP1") AS "TEMP(Calculation_369576668395024390)(2478313272)(0)",   SUM("SalariesFrance_8"."AG_M25") AS "TEMP(Calculation_369576668395024390)(3700677619)(0)",   SUM("SalariesFrance_8"."FEMMES") AS "TEMP(Calculation_369576668396457992)(2123450049)(0)",   SUM("SalariesFrance_8"."EMPSAL_NP1") AS "TEMP(Calculation_369576668396457992)(3149183137)(0)",   SUM("SalariesFrance_8"."SALAIRE_VF") AS "TEMP(Calculation_393783518251319297)(57485518)(0)",   COUNT("SalariesFrance_8"."SALAIRE_VF") AS "TEMP(Calculation_393783518251319297)(879651027)(0)",   SUM("SalariesFrance_8"."AG_30_39") AS "TEMP(Entre 25 et 29 ans (copie))(3219587110)(0)",   SUM(("SalariesFrance_8"."AG_40_49" + "SalariesFrance_8"."AG_50_54")) AS "TEMP(Entre 30 et 39 ans (copie))(3826808445)(0)",   SUM("SalariesFrance_8"."AG_P55") AS "TEMP(Moins de 25 ans (copie 2))(3051566802)(0)",   SUM("SalariesFrance_8"."AG_25_29") AS "TEMP(Moins de 25 ans (copie))(341663134)(0)",   SUM("SalariesFrance_8"."HOMMES") AS "TEMP(des femmes (copie))(64173573)(0)",   AVG(CAST(CAST("SalariesFrance_8"."Calculation_163536984210948109" AS BIGINT) AS double)) AS "avg:Calculation_163536984210948109:ok",   AVG(CAST(CAST("SalariesFrance_8"."REPERE1 (copie)" AS BIGINT) AS double)) AS "avg:REPERE1 (copie):ok",   CAST(MIN("SalariesFrance_8"."Calculation_163536984210948109") AS BIGINT) AS "min:Calculation_163536984210948109:ok",   CAST(MIN("SalariesFrance_8"."REPERE1 (copie)") AS BIGINT) AS "min:REPERE1 (copie):ok" FROM "SalariesFrance_8" HAVING (COUNT(1) > 0) ORDER BY "TEMP(Calculation_369576668387979265)(1183172592)(0)";
