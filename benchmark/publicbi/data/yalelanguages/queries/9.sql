SELECT "YaleLanguages_3"."Calculation_3110108110633423" AS "Calculation_3110108110633423",   "YaleLanguages_3"."Patron Group" AS "Patron Group" FROM "YaleLanguages_3" WHERE ((CAST("YaleLanguages_3"."CHARGE_DATE" as DATE) >= cast('2002-01-01' as DATE)) AND ("YaleLanguages_3"."PATRON_TYPE (Pseudo vs Patron)" = 'Patron')) GROUP BY "Calculation_3110108110633423", "Patron Group" ORDER BY "Calculation_3110108110633423";
