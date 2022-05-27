SELECT "CMSprovider_2"."HCPCS_DESCRIPTION" AS "HCPCS_DESCRIPTION",   CAST("CMSprovider_2"."NPPES_PROVIDER_ZIP" AS BIGINT) AS "NPPES_PROVIDER_ZIP",   AVG(CAST("CMSprovider_2"."AVERAGE_SUBMITTED_CHRG_AMT" AS double)) AS "avg:AVERAGE_SUBMITTED_CHRG_AMT:ok" FROM "CMSprovider_2" WHERE (("CMSprovider_2"."HCPCS_DESCRIPTION" IN ('Removal of joint lining from two or more knee joint compartments using an endoscope', 'Removal of knee cap', 'Removal of knee joint covering')) AND ("CMSprovider_2"."NPPES_PROVIDER_STATE" = 'CA')) GROUP BY "CMSprovider_2"."HCPCS_DESCRIPTION",   "CMSprovider_2"."NPPES_PROVIDER_ZIP",   "CMSprovider_2"."NPPES_PROVIDER_ZIP" ORDER BY "HCPCS_DESCRIPTION";
