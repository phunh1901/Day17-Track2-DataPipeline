# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Học viên  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 141.2s
  run 2/3 … 138.6s
  run 3/3 … 139.1s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Phiếu sự cố #1041: Khi job lỗi mạng, bấm Clear Task trên Airflow cho chạy lại thì `gold_training_set` tăng số hàng sau mỗi lần chạy mà không báo lỗi. Bị lặp 12,480 ticket (38,750 hàng sau 3 lượt chạy so với 12,480 kỳ vọng). |
| **Nguyên nhân** | Model `gold_training_set.sql` được cấu hình `materialized = 'incremental'` nhưng **không khai báo `unique_key` và `incremental_strategy`**, khiến dbt mặc định sử dụng chiến lược `append` (sinh câu lệnh `INSERT INTO`). Khi nguồn CDC chứa các bản ghi cập nhật (`op = 'u'`) diễn ra ở nhiều ngày khác nhau, hoặc khi Clear Task trên DAG đang để `catchup=True` và thiếu `max_active_runs`, các bản ghi có cùng `ticket_id` lọt qua điều kiện lọc `run_date` nhiều lần và bị ghi thêm (append) vào đích thay vì ghi đè. |
| **Cách khắc phục** | • Trong `dbt/models/gold/gold_training_set.sql`: Khai báo `unique_key = 'ticket_id'` và `incremental_strategy = 'delete+insert'`.<br>• Trong `dags/ai_training_pipeline.py`: Cấu hình `catchup=False` và `max_active_runs=1`. |
| **Bằng chứng** | trước: 38,750 hàng (12,480 ticket bị lặp) · sau: 12,480 hàng · checksum 3 lượt: giống hệt nhau (✓ ok, 0 ticket lặp). |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Phiếu sự cố #1043: `gold_feature_daily` thiếu khoảng 5% số hàng so với đối chiếu thủ công; chỉ thiếu ở những ngày quá khứ đã chạy xong từ lâu, ngày mới thì đủ (8,645 / 9,100 hàng). |
| **P99 độ trễ đo được** | **2.73 ngày** *(chính xác: 2.726 ngày, tỷ lệ sự kiện trễ > 1 ngày là 5.05%)* |
| **Lookback đã chọn** | 3 ngày — vì bao phủ hoàn toàn ngưỡng P99 độ trễ (2.73 ngày) của dữ liệu đến muộn (`_ingested_at > event_time`), đảm bảo thu nạp >99% sự kiện bị trễ mà không gây dư thừa chi phí quét. |
| **Nguyên nhân** | Mệnh đề `is_incremental()` ban đầu dùng điều kiện `where event_date > (select max(event_date) from {{ this }})` chỉ xử lý các sự kiện có `event_date` lớn hơn ngày lớn nhất đã có trong bảng đích. Một sự kiện xảy ra ở quá khứ (ví dụ `event_date = 08-12`) nhưng đến kho muộn (ví dụ `_ingested_at = 08-15`) sẽ bị loại bỏ hoàn toàn do tại ngày 08-15, `max(event_date)` trong đích đã là ngày lớn hơn (08-14 hoặc 08-15). |
| **Cách khắc phục** | • Thêm lookback window: `where event_date >= (select max(event_date) - interval 3 day from {{ this }})`.<br>• Khai báo `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'delete+insert'` trong `dbt/models/gold/gold_feature_daily.sql` để tính lại và ghi đè các ngày trong window thay vì cộng dồn. |
| **Bằng chứng** | trước: 8,645 hàng · sau: 9,100 hàng (đúng 14 ngày × 650 khách hàng, ổn định 3 lượt chạy). |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> Nếu chọn `max` (hoặc quét toàn bộ lịch sử), window sẽ bị kéo dãn vô hạn bởi một vài outlier cá biệt (ví dụ dữ liệu bị kẹt hàng tháng do lỗi mạng/hạ tầng), làm tăng vọt chi phí tính toán (compute, IO, scan volume) ở **mọi** lượt chạy định kỳ trong tương lai. Chọn P99 cân bằng tối ưu giữa việc bao phủ hầu hết dữ liệu thực tế (>99%) với chi phí tài nguyên cố định và nhỏ gọn ở mỗi chu kỳ (chỉ tính lại 3 ngày thay vì toàn bộ lịch sử). Đối với 1% outlier cực hiếm còn lại, hệ thống có thể xử lý qua quy trình đối soát định kỳ (weekly/monthly reconciliation batch).

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Phiếu sự cố #1047: Backend đổi kiểu cột `priority` từ số sang chuỗi từ ngày 08-10. Pipeline không dừng nhưng mô hình phân loại dự đoán kém hẳn do cột priority trong Silver bị NULL (6,606 hàng sai). |
| **Nguyên nhân** | Macro ban đầu dùng `try_cast(priority_raw as integer)` làm biến toàn bộ nhãn chuỗi hợp lệ (`urgent`, `high`, `medium`, `low`) thành `NULL`, đồng thời chấp nhận sai các giá trị ngoài miền (`0`, `5`, `-1`). Ngoài ra, `silver_tickets.sql` xếp hạng `row_number()` trước khi lọc lỗi dẫn đến ticket có bản ghi mới nhất bị lỗi sẽ bị loại bỏ hoàn toàn khỏi Silver (mất ticket). Data contract bị tắt (`enforced: false`) và thiếu test miền giá trị. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | 1. **Nhóm 1 (Số hợp lệ):** `'1'`, `'2'`, `'3'`, `'4'` -> Giữ nguyên ép về INTEGER.<br>2. **Nhóm 2 (Nhãn chuỗi - Schema Evolution):** `'urgent'`, `'high'`, `'medium'`, `'low'` -> Ánh xạ (Map) về số tương ứng: `urgent→1`, `high→2`, `medium→3`, `low→4`.<br>3. **Nhóm 3 (Dữ liệu lỗi thực sự):** `'P1'`, `'unknown'`, `'0'`, `'5'`, `'-1'`, `''`, `NULL` -> Trả về `NULL` trong macro để đưa vào `quarantine_tickets`. |
| **Cách khắc phục** | • `dbt/macros/normalize_priority.sql`: Dùng `CASE WHEN` xử lý 3 nhóm.<br>• `dbt/models/silver/silver_tickets.sql`: Lọc bản ghi hợp lệ *trước* khi đánh số thứ tự (`where priority_clean is not null`) để bảo toàn trạng thái trước đó của ticket.<br>• `dbt/models/silver/quarantine_tickets.sql`: Lọc `where priority_clean is null`.<br>• `dbt/models/silver/schema.yml`: Bật `contract: enforced: true` và thêm test `accepted_values: [1, 2, 3, 4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng · `silver_tickets` = 12,480 tickets (0 hàng sai) · `dbt test` 11/11 pass. |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> 1. **Nên tiếp nhận toàn bộ ở Bronze và validate/chặn/quarantine ở tầng Silver:** Nếu chặn và từ chối ghi nhận ngay tại Bronze, dữ liệu thô gốc sẽ bị mất vĩnh viễn (data loss). Đội ngũ Data Platform sẽ không thể audit, điều tra nguyên nhân gốc rễ, hay thực hiện replay/backfill khi có quy tắc mapping mới.
> 2. **Không nên để pipeline dừng (fail DAG) khi gặp bản ghi lỗi:** Một tỷ lệ nhỏ bản ghi lỗi (312 hàng) không được phép làm gián đoạn toàn bộ hơn 130,000 sự kiện và 31,200 chunk tài liệu hợp lệ đang phục vụ các hệ thống downstream (RAG index, routing agent, dashboard). Việc định tuyến bản ghi lỗi vào hàng đợi Quarantine (Dead Letter Queue) vừa đảm bảo tính sẵn sàng cao (High Availability) cho hệ thống, vừa gom các bản ghi lỗi tập trung để kỹ sư xử lý riêng biệt.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | Cả 2 bài A và B |
| **Nguyên nhân** | • **Bài A:** 5.000 file Parquet nhỏ (small-file problem) khiến engine đọc theo lô làm tròn công quét lên 5.000.000 rows scanned; predicate `strftime(event_time, '%Y-%m-%d')` không sargable nên không dùng được partition pruning.<br>• **Bài B:** Consumer commit offset trước khi ghi dữ liệu (At-most-once), khi bị kill giữa batch sẽ làm mất dữ liệu; câu lệnh INSERT thuần gây trùng lặp khi replay. |
| **Cách khắc phục** | • **Bài A:** Chạy `tools/compact.py` gom 5.000 file thành 14 file partition theo `event_date`, sắp xếp theo `customer_name, event_time`, `row_group_size = 100000`; viết lại `queries/dashboard.sql` dùng predicate sargable `event_date = '2026-08-09'`.<br>• **Bài B:** Chuyển sang At-least-once (ghi `write_batch` trước, commit offset sau); thêm `PRIMARY KEY (event_id)` và `ON CONFLICT (event_id) DO UPDATE SET ...` trong `write_batch`, đồng thời gọi `checkpoint` đồng bộ WAL. |
| **Bằng chứng** | • **Bài A:** `rows scanned` giảm từ 5,000,000 xuống 9,324 (**giảm 536.3×**, yêu cầu ≥ 10×), số file giảm từ 5,000 xuống 14, `result hash` giữ nguyên `4379e4c5d9f3`.<br>• **Bài B:** `make crash-test` đạt: 0 mất hàng, 0 trùng hàng, C == A (20,000 hàng). |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Kiểm tra tính idempotent của các model incremental (đã có `unique_key` và `incremental_strategy` phù hợp với grain và kiểu dữ liệu CDC chưa) cùng cấu hình concurrency/catchup của orchestrator (Airflow). |
| 2 | Đo lường phân bố độ trễ (P95/P99 latency) giữa thời điểm phát sinh sự kiện và thời điểm nhập kho để thiết lập lookback window phù hợp cho các pipeline incremental, tránh thất thoát dữ liệu về muộn. |
| 3 | Kiểm tra Data Contract (`contract: enforced: true`), các test kiểm tra miền giá trị, và cơ chế Dead Letter Queue / Quarantine để định tuyến dữ liệu lỗi thay vì để lỗi format làm sập toàn bộ hệ thống downstream. |
