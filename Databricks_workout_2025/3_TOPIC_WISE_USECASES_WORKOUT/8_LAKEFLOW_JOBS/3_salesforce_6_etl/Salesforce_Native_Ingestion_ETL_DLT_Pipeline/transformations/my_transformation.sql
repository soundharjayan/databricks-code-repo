CREATE OR REFRESH STREAMING TABLE soundhar_catalog.lakeflow_job_schema.account_silver
AS
SELECT
  *,
  upper(Industry) AS Industry_upper
FROM STREAM(soundhar_catalog.default.account);