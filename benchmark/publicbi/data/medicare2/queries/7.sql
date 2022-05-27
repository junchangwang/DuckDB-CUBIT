SELECT CAST("Medicare2_2"."bene_unique_cnt" AS BIGINT) AS "bene_unique_cnt (copy)",   CAST("Medicare2_2"."hcpcs_code" AS BIGINT) AS "hcpcs_code",   "Medicare2_2"."hcpcs_description" AS "hcpcs_description",   CAST("Medicare2_2"."line_srvc_cnt" AS BIGINT) AS "line_srvc_cnt (copy)",   "Medicare2_2"."nppes_provider_city" AS "nppes_provider_city",   "Medicare2_2"."nppes_provider_first_name" AS "nppes_provider_first_name",   "Medicare2_2"."nppes_provider_last_org_name" AS "nppes_provider_last_org_name",   "Medicare2_2"."nppes_provider_street1" AS "nppes_provider_street1",   CAST("Medicare2_2"."nppes_provider_zip" AS BIGINT) AS "nppes_provider_zip",   SUM((CAST("Medicare2_2"."average_Medicare_payment_amt" AS double) * CAST(CAST("Medicare2_2"."line_srvc_cnt" AS BIGINT) AS double))) AS "sum:total_Medicare_payment (copy):ok",   (CAST("Medicare2_2"."average_Medicare_payment_amt" AS double) * CAST(CAST("Medicare2_2"."line_srvc_cnt" AS BIGINT) AS double)) AS "total_Medicare_payment (copy)" FROM "Medicare2_2" WHERE (("Medicare2_2"."nppes_provider_state" = 'NY') AND ("Medicare2_2"."nppes_provider_country" = 'US') AND ("Medicare2_2"."provider_type" = 'Allergy/Immunology')) GROUP BY "Medicare2_2"."bene_unique_cnt",   "Medicare2_2"."hcpcs_code",   "Medicare2_2"."hcpcs_description",   "Medicare2_2"."line_srvc_cnt",   "Medicare2_2"."nppes_provider_city",   "Medicare2_2"."nppes_provider_first_name",   "Medicare2_2"."nppes_provider_last_org_name",   "Medicare2_2"."nppes_provider_street1",   "Medicare2_2"."nppes_provider_zip",   "total_Medicare_payment (copy)",   "bene_unique_cnt (copy)",   "hcpcs_code",   "line_srvc_cnt (copy)",   "nppes_provider_zip" ORDER BY "bene_unique_cnt (copy)";
