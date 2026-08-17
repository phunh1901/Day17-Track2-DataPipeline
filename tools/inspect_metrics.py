import duckdb

con = duckdb.connect("warehouse.duckdb")
print("=== P99 LATENCY METRICS ===")
con.sql("""
select
    round(quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.50), 3) as p50_ngay,
    round(quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.95), 3) as p95_ngay,
    round(quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.99), 3) as p99_ngay,
    round(max(date_diff('second', event_time, _ingested_at)/86400.0), 3)                 as max_ngay,
    round(avg(case when _ingested_at > event_time + interval 1 day then 1.0 else 0 end), 4) as ty_le_late
from bronze_events;
""").show()

print("=== PRIORITY RAW DISTRIBUTION ===")
con.sql("""
select priority_raw, count(*) as count
from bronze_tickets_cdc
group by 1 order by 2 desc;
""").show(max_rows=30)
