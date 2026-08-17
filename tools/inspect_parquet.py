import duckdb

con = duckdb.connect()
print("Parquet columns:")
con.sql("describe select * from read_parquet('data/gold_events/*.parquet') limit 1").show()

print("Sample row:")
con.sql("select * from read_parquet('data/gold_events/*.parquet') limit 2").show()

print("Total rows:")
con.sql("select count(*) from read_parquet('data/gold_events/*.parquet')").show()
