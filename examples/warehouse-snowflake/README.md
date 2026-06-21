# Build a Snowflake warehouse from raw CSVs

A CRM export lands as two flat files — `data/customers.csv` and `data/orders.csv`.
This walks dacli through standing up a **Bronze → Silver → Gold** warehouse in
Snowflake: raw load, cleaned core, analytics marts. Every write is classified,
gated, and confirmed against Snowflake before the next step runs — and the last
section proves rollback with Time Travel.

The source data is deliberately dirty: a duplicate customer (`C002` twice, with
`USA` vs `usa`), a null email (`C004`), whitespace and mixed-case emails, mixed
country spellings, a null order amount (`O1015`), and an order for a customer who
doesn't exist (`O1012 → C999`). Silver is where that gets fixed; the data-quality
gate is where the orphan gets caught.

## What you need

- dacli installed (`pipx install dacli`, or from source — see the root README).
- A Snowflake account with a role that can `CREATE SCHEMA`/`CREATE TABLE` in one
  database, and a warehouse you can resume.
- The Snowflake connector enabled:

  ```bash
  dacli setup --profile snowflake_only     # paste account/user/password/warehouse/database
  dacli validate                           # live credential check
  ```

No cloud account? Skip to [Preview it offline](#preview-it-offline) — you'll see
the governed plan without touching Snowflake.

## The shape we're building

```text
RAW    (Bronze)   CUSTOMERS, ORDERS         everything VARCHAR, loaded verbatim + _loaded_at
CORE   (Silver)   CUSTOMERS, ORDERS         typed, deduped, trimmed, country normalized
MART   (Gold)     CUSTOMER_REVENUE,         lifetime paid revenue per customer,
                  DAILY_REVENUE             revenue per day
```

## 1 — Bronze: land the files verbatim

Start dacli (`dacli`) and ask:

> Create a RAW schema, then a RAW.CUSTOMERS and RAW.ORDERS table with every
> column as VARCHAR plus a `_loaded_at` timestamp, and load the two CSVs in
> `examples/warehouse-snowflake/data/` into them.

dacli plans it, then loads through a Snowflake internal stage:

```sql
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE TABLE RAW.CUSTOMERS (
  customer_id STRING, full_name STRING, email STRING,
  country STRING, plan STRING, signup_date STRING,
  _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
PUT file://data/customers.csv @~/dacli_demo OVERWRITE=TRUE;
COPY INTO RAW.CUSTOMERS (customer_id, full_name, email, country, plan, signup_date)
  FROM @~/dacli_demo/customers.csv
  FILE_FORMAT = (TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');
```

`CREATE TABLE` carries an environment-anchored post-condition: dacli reads
`INFORMATION_SCHEMA.COLUMNS` back and refuses to call the step done unless the
live column set matches what it intended. The `COPY INTO` is confirmed by the
row count Snowflake returns against the file's line count — fluent "success" is
never the proof.

## 2 — Silver: clean and conform

> Build a CORE schema. From RAW, produce CORE.CUSTOMERS — one row per customer
> (keep the latest by signup_date), trim the emails, lower-case them, and
> normalize country to a canonical name. Then CORE.ORDERS typed properly, with
> amount_usd as a NUMBER and order_ts as a TIMESTAMP.

The dedup and conform logic dacli writes:

```sql
CREATE SCHEMA IF NOT EXISTS CORE;

CREATE OR REPLACE TABLE CORE.CUSTOMERS AS
SELECT
  customer_id,
  full_name,
  LOWER(TRIM(email))                              AS email,
  CASE UPPER(TRIM(country))
    WHEN 'USA'           THEN 'United States'
    WHEN 'UNITED STATES' THEN 'United States'
    WHEN 'UK'            THEN 'United Kingdom'
    ELSE INITCAP(country)
  END                                             AS country,
  plan,
  TO_DATE(signup_date)                            AS signup_date
FROM RAW.CUSTOMERS
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY customer_id ORDER BY signup_date DESC
) = 1;

CREATE OR REPLACE TABLE CORE.ORDERS AS
SELECT
  order_id,
  customer_id,
  TO_TIMESTAMP_NTZ(order_ts)  AS order_ts,
  TRY_TO_NUMBER(amount_usd)   AS amount_usd,
  status
FROM RAW.ORDERS;
```

`C002` collapses to one row; `usa`/`USA`/`United States` all land as
`United States`. Because `CREATE OR REPLACE` on a populated table is reversible
only through Time Travel, dacli tiers it **write** (not irreversible) — Snowflake
keeps the prior version for the retention window — and verifies the new row count
is sane before continuing.

## 3 — Gold: the marts

> Create a MART schema with CUSTOMER_REVENUE (per customer: total paid revenue,
> order count, country) and DAILY_REVENUE (per day: paid revenue, order count).
> Only count orders with status = 'paid'.

```sql
CREATE SCHEMA IF NOT EXISTS MART;

CREATE OR REPLACE TABLE MART.CUSTOMER_REVENUE AS
SELECT
  c.customer_id,
  c.full_name,
  c.country,
  COUNT_IF(o.status = 'paid')                              AS paid_orders,
  COALESCE(SUM(CASE WHEN o.status='paid' THEN o.amount_usd END), 0) AS revenue_usd
FROM CORE.CUSTOMERS c
LEFT JOIN CORE.ORDERS o USING (customer_id)
GROUP BY 1, 2, 3;

CREATE OR REPLACE TABLE MART.DAILY_REVENUE AS
SELECT
  TO_DATE(order_ts)        AS order_date,
  SUM(amount_usd)          AS revenue_usd,
  COUNT(*)                 AS orders
FROM CORE.ORDERS
WHERE status = 'paid'
GROUP BY 1
ORDER BY 1;
```

## 4 — Gate the data before anyone trusts it

The orphan order (`O1012 → C999`) and the null email are exactly the kind of
thing that quietly corrupts a mart. Catch them with assertions that exit non-zero
so CI can block a bad load:

```bash
# No NULL emails in the conformed customer table.
dacli assert define core-email-not-null \
  --connector snowflake --object CORE.CUSTOMERS \
  --metric null_rate --column email --max 0

# Every order points at a real customer (expressed as a null-rate after a join view).
dacli assert run core-email-not-null
```

A breach prints the measured metric and proposes a remediation; `--apply` routes
that fix through the same classify → approve → verify → rollback gate as any other
write. Nothing changes on a bare `run`.

## 5 — Know the cost before you run it

```bash
dacli cost snowflake --estimate "SELECT * FROM CORE.ORDERS o JOIN CORE.CUSTOMERS c USING (customer_id)"
dacli cost snowflake --session        # what this session actually spent, from the warehouse history view
```

Set `governance.cost_confirm_usd` in your config and any step whose estimated
cost clears the threshold needs a human confirm, with the estimate shown in the
approval panel.

## 6 — Break it, then undo it (Time Travel)

This is the part the architecture is built around. Drop a Gold table:

> Drop MART.DAILY_REVENUE.

`DROP TABLE` classifies **irreversible**. dacli does not take the model's word
that this is safe — it checks that a native undo path exists first. Snowflake
Time Travel gives one (`UNDROP`), so the action is gated behind an explicit
confirm rather than blocked outright. Approve it, then:

> Actually, bring MART.DAILY_REVENUE back.

```sql
UNDROP TABLE MART.DAILY_REVENUE;
```

dacli confirms the restore by reading the table back out of
`INFORMATION_SCHEMA` — the same oracle it used to confirm the create. If Time
Travel retention had been zero, the drop would have been **blocked**, not
confirmed, because the rollback path could not be verified to exist.

## Preview it offline

No Snowflake handy? `plan` decomposes the whole job and shows the per-step blast
radius with zero credentials and no LLM call:

```bash
dacli plan "load two CSVs into Snowflake RAW, build CORE and MART schemas, drop MART.DAILY_REVENUE"
```

Or replay the scripted version of the build — a fixed transcript with a scripted
model, so it runs hermetically:

```bash
dacli replay examples/warehouse-snowflake/scenario.json
```

## Verified run (real Snowflake)

This example was run end to end against a live Snowflake account — driven by
`dacli`, every write through the governed dispatch path. Captured results:

```text
layer counts   RAW 18/22  ->  CORE 17/22 (C002 deduped)  ->  MART 17 / 19 daily
top customer   Wei Zhang · China · 3 paid orders · $447
total paid     $1,933 across 17 customers
countries      10 canonical (usa/USA/United States -> United States, uk -> United Kingdom, ...)
quality gate   1 null email (C004) · 1 orphan order (O1012 -> C999)  — both caught
rollback       DROP MART.DAILY_REVENUE gated (Time Travel = verified rollback);
               restored with UNDROP -> 19 rows back
```

One honest caveat: `dacli run` is **headless**, and Snowflake's `UNDROP` itself
classifies as irreversible-without-rollback, so the agent pauses for a human
confirm it can't get without a TTY. In interactive `dacli` you approve it inline;
here the restore was issued directly. The `DROP` gating — the part the
architecture is built around — runs headlessly as shown.

## A real planning turn

Asked to outline the Bronze load headlessly (`dacli run "…" --approve deny`,
nothing executed), the agent lands on the same shape this walkthrough uses —
stage + `COPY INTO`, then verify:

> The table must exist (verified via `introspect_snowflake_object` for schema
> `RAW` and table `CUSTOMERS`) and the `row_count` … must be greater than 0 and
> equal to the number of data rows in `customers.csv` (total lines minus the
> header). This confirms both the table's creation and that the CSV was fully
> ingested.

## Files

```text
examples/warehouse-snowflake/
├── README.md            # this walkthrough
├── data/
│   ├── customers.csv     # 18 rows, intentionally dirty
│   └── orders.csv        # 22 rows, one orphan + one null amount
├── scenario.json         # hermetic `dacli replay` of the build (no creds)
└── .env.example          # the Snowflake settings the connector reads
```
