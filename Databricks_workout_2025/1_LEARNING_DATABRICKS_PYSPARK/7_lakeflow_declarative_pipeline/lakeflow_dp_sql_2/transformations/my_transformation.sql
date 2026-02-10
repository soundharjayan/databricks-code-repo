CREATE OR REFRESH STREAMING TABLE catalog1_dropme.default.drugstbl_bronze2
AS
SELECT *
FROM STREAM(lakehousecat.deltadb.drugstbl);