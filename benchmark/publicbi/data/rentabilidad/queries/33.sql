SELECT "Rentabilidad_9"."Ruta de Venta" AS "Ruta de Venta",   SUM("Rentabilidad_9"."IN") AS "TEMP(Calculation_0070818164712315)(1653230849)(0)",   SUM("Rentabilidad_9"."CF") AS "TEMP(Calculation_0070818164712315)(3669921802)(0)",   SUM("Rentabilidad_9"."TOTAL MERCADEO") AS "TEMP(Calculation_0950818192405729)(610974675)(0)",   SUM("Rentabilidad_9"."TOTAL REPARTO") AS "TEMP(Calculation_2160818192423622)(3492347901)(0)",   SUM("Rentabilidad_9"."TOTAL BODEGA C/ARRIENDOS") AS "TEMP(Calculation_3070818192539610)(2359430539)(0)",   SUM("Rentabilidad_9"."TOTAL T1") AS "TEMP(Calculation_4790818192442873)(2729409475)(0)",   SUM("Rentabilidad_9"."Rentabilidad") AS "TEMP(Calculation_5560818164729849)(3482281234)(0)",   SUM("Rentabilidad_9"."TOTAL VENTA") AS "TEMP(Calculation_7680818192512481)(293833081)(0)",   COUNT(DISTINCT "Rentabilidad_9"."Deudor") AS "ctd:Deudor:ok" FROM "Rentabilidad_9" WHERE (("Rentabilidad_9"."Figura" = 'Preventa On Premise') AND ("Rentabilidad_9"."Sede Foraneo Sintec" = 'Sede') AND ("Rentabilidad_9"."Zona" = 'OC')) GROUP BY "Rentabilidad_9"."Ruta de Venta" ORDER BY "Ruta de Venta";
