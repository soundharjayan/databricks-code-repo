CREATE OR REFRESH STREAMING TABLE soundhar_catalog.logistics_dab_schema.account_silver
AS
SELECT
  *,
  upper(Industry) AS Industry_upper
FROM STREAM(soundhar_catalog.default.account);