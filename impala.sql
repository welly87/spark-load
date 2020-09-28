CREATE EXTERNAL TABLE sfmta_kudu
STORED AS KUDU
TBLPROPERTIES('kudu.table_name' = 'sfmta_kudu');

SELECT * FROM sfmta_kudu
ORDER BY speed
LIMIT 5;
