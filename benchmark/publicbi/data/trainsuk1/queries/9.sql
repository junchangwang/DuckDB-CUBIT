SELECT MIN("TrainsUK1_4"."Calculation_2480421151322357") AS "TEMP(attr:Calculation_2480421151322357:nk)(3539376641)(0)",   MAX("TrainsUK1_4"."Calculation_2480421151322357") AS "TEMP(attr:Calculation_2480421151322357:nk)(3547210890)(0)",   MIN("TrainsUK1_4"."Section Start Location Full Name") AS "TEMP(attr:Section Start Location Full Name:nk)(1915377183)(0)",   MAX("TrainsUK1_4"."Section Start Location Full Name") AS "TEMP(attr:Section Start Location Full Name:nk)(4245200940)(0)",   MIN(CAST("TrainsUK1_4"."v_Section_WTT_Time" as TIME)) AS "TEMP(attr:v_Section_WTT_Time:ok)(2639448957)(0)",   MAX(CAST("TrainsUK1_4"."v_Section_WTT_Time" as TIME)) AS "TEMP(attr:v_Section_WTT_Time:ok)(881973835)(0)",   SUM("TrainsUK1_4"."Calculation_2040623161253421") AS "sum:Calculation_2040623161253421:ok",   "TrainsUK1_4"."v_WTT and Section Name and Timing Event" AS "v_WTT and Section Name and Timing Event" FROM "TrainsUK1_4" WHERE (('' = '') AND (NOT ("TrainsUK1_4"."Timetable" IN ('', 'Timetable'))) AND ("TrainsUK1_4"."Operator" = 'EB') AND ("TrainsUK1_4"."Timetable" = 'M18') AND ("TrainsUK1_4"."v_Headcode Description" = '1A04London Liverpool Street to Harwich Town at 06:38')) GROUP BY "v_WTT and Section Name and Timing Event" ORDER BY "TEMP(attr:Calculation_2480421151322357:nk)(3539376641)(0)";
