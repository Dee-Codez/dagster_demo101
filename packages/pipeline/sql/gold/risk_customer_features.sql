CREATE SCHEMA IF NOT EXISTS gold_{schema_prefix};

DROP TABLE IF EXISTS gold_{schema_prefix}.risk_customer_features;

CREATE TABLE gold_{schema_prefix}.risk_customer_features AS
SELECT
    c.customer_id,
    c.segment,
    c.country,
    EXTRACT(DAY FROM NOW() - c.onboarded_at)::INT           AS onboarded_days_ago,
    COUNT(DISTINCT a.account_id)                            AS account_count,
    COALESCE(SUM(a.balance), 0)                             AS total_balance,
    COALESCE(AVG(a.balance), 0)                             AS avg_balance,
    COUNT(t.txn_id) FILTER (
        WHERE t.txn_at >= NOW() - INTERVAL '30 days'
    )                                                       AS txn_count_30d,
    COUNT(t.txn_id) FILTER (
        WHERE t.txn_at >= NOW() - INTERVAL '90 days'
    )                                                       AS txn_count_90d,
    COALESCE(SUM(t.amount) FILTER (
        WHERE t.txn_at >= NOW() - INTERVAL '90 days'
    ), 0)                                                   AS txn_amount_sum_90d,
    COALESCE(AVG(t.amount) FILTER (
        WHERE t.txn_at >= NOW() - INTERVAL '90 days'
    ), 0)                                                   AS txn_amount_avg_90d,
    EXTRACT(DAY FROM NOW() - MAX(t.txn_at))::INT            AS days_since_last_txn,
    COUNT(r.event_id) FILTER (
        WHERE r.resolved_at IS NULL
    )                                                       AS open_risk_events,
    COALESCE(MAX(
        CASE r.severity
            WHEN 'critical' THEN 4
            WHEN 'high'     THEN 3
            WHEN 'medium'   THEN 2
            WHEN 'low'      THEN 1
            ELSE 0
        END
    ), 0)                                                   AS max_severity_score
FROM silver_{schema_prefix}.customers c
LEFT JOIN silver_{schema_prefix}.accounts    a ON c.customer_id = a.customer_id
LEFT JOIN silver_{schema_prefix}.transactions t ON a.account_id = t.account_id
LEFT JOIN silver_{schema_prefix}.risk_events r ON c.customer_id = r.customer_id
GROUP BY c.customer_id, c.segment, c.country, c.onboarded_at;
