CREATE OR REFRESH STREAMING TABLE catalog1_we47.schema1_we47.account_silver
AS
SELECT
  *,
  upper(Industry) AS Industry_upper
FROM STREAM(catalog1_we47.schema1_we47.account);