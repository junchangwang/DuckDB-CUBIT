SELECT MIN("Taxpayer_10"."hcpcs_description") AS "TEMP(attr:hcpcs_description:nk)(2092202119)(0)",   MAX("Taxpayer_10"."hcpcs_description") AS "TEMP(attr:hcpcs_description:nk)(3741561916)(0)",   AVG(CAST(("Taxpayer_10"."average_Medicare_allowed_amt" - "Taxpayer_10"."average_Medicare_payment_amt") AS double)) AS "avg:Calculation_9940518082838207:ok",   CAST("Taxpayer_10"."npi" AS BIGINT) AS "npi",   "Taxpayer_10"."nppes_provider_street1" AS "nppes_provider_street1" FROM "Taxpayer_10" WHERE (("Taxpayer_10"."hcpcs_description" = 'Initial hospital care') AND ("Taxpayer_10"."nppes_provider_state" = 'WA')) GROUP BY "Taxpayer_10"."npi",   "Taxpayer_10"."nppes_provider_street1",   "npi" ORDER BY "TEMP(attr:hcpcs_description:nk)(2092202119)(0)";
