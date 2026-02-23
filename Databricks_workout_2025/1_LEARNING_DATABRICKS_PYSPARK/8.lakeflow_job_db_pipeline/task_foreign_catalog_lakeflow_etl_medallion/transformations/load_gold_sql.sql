CREATE OR REFRESH MATERIALIZED VIEW catalog1_we47.schema1_we47.gold_shipments_agg
AS
SELECT
  role,avg(age) avgage,count(1) cnt
FROM catalog1_we47.schema1_we47.silver_shipments_dlt1
GROUP BY role;