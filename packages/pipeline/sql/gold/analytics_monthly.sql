CREATE SCHEMA IF NOT EXISTS gold_{schema_prefix};

DROP TABLE IF EXISTS gold_{schema_prefix}.analytics_monthly;

CREATE TABLE gold_{schema_prefix}.analytics_monthly AS
WITH monthly_txns AS (
    SELECT
        a.customer_id,
        TO_CHAR(t.txn_at, 'YYYY-MM')  AS year_month,
        COUNT(t.txn_id)                AS txn_count,
        COALESCE(SUM(t.amount), 0)     AS txn_amount_sum,
        COALESCE(AVG(t.amount), 0)     AS txn_amount_avg
    FROM silver_{schema_prefix}.transactions t
    JOIN silver_{schema_prefix}.accounts a ON t.account_id = a.account_id
    GROUP BY a.customer_id, TO_CHAR(t.txn_at, 'YYYY-MM')
),
monthly_accounts AS (
    SELECT
        customer_id,
        TO_CHAR(opened_at, 'YYYY-MM')  AS year_month,
        COUNT(account_id)               AS new_accounts_opened
    FROM silver_{schema_prefix}.accounts
    GROUP BY customer_id, TO_CHAR(opened_at, 'YYYY-MM')
),
monthly_risk AS (
    SELECT
        customer_id,
        TO_CHAR(occurred_at, 'YYYY-MM')  AS year_month,
        COUNT(event_id)                   AS risk_events_count,
        COUNT(event_id) FILTER (
            WHERE resolved_at IS NOT NULL
              AND TO_CHAR(resolved_at, 'YYYY-MM') = TO_CHAR(occurred_at, 'YYYY-MM')
        )                                 AS risk_events_resolved
    FROM silver_{schema_prefix}.risk_events
    GROUP BY customer_id, TO_CHAR(occurred_at, 'YYYY-MM')
)
SELECT
    c.customer_id,
    mt.year_month,
    c.segment,
    c.country,
    mt.txn_count,
    mt.txn_amount_sum,
    mt.txn_amount_avg,
    COALESCE(ma.new_accounts_opened, 0)  AS new_accounts_opened,
    COALESCE(mr.risk_events_count, 0)    AS risk_events_count,
    COALESCE(mr.risk_events_resolved, 0) AS risk_events_resolved
FROM silver_{schema_prefix}.customers c
JOIN monthly_txns mt
    ON c.customer_id = mt.customer_id
LEFT JOIN monthly_accounts ma
    ON c.customer_id = ma.customer_id AND mt.year_month = ma.year_month
LEFT JOIN monthly_risk mr
    ON c.customer_id = mr.customer_id AND mt.year_month = mr.year_month;
