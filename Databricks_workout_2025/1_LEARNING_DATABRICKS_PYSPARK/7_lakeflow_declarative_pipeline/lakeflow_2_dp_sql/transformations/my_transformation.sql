CREATE OR REFRESH STREAMING TABLE catalog1_dropme.default.drugstbl_bronzetbl1
AS
SELECT *
FROM STREAM(lakehousecat.deltadb.drugstbl);