SELECT "MedPayment1_1"."hcpcs_code" AS "hcpcs_code",   "MedPayment1_1"."hcpcs_description" AS "hcpcs_description",   "MedPayment1_1"."nppes_provider_state" AS "nppes_provider_state",   SUM((CAST("MedPayment1_1"."average_Medicare_payment_amt" AS double) * CAST(CAST("MedPayment1_1"."Number of Records" AS BIGINT) AS double))) AS "sum:Calculation_0820513143749095:ok",   SUM("MedPayment1_1"."average_submitted_chrg_amt") AS "sum:average_submitted_chrg_amt:ok" FROM "MedPayment1_1" WHERE (("MedPayment1_1"."hcpcs_code" = '27447') AND ("MedPayment1_1"."hcpcs_description" = 'Total knee arthroplasty')) GROUP BY "MedPayment1_1"."hcpcs_code",   "MedPayment1_1"."hcpcs_description", "MedPayment1_1"."nppes_provider_state" ORDER BY "hcpcs_code";
