{{ config(
    alias='risk_event_silver_layer',
    materialized='table',
    tags='silver_layer'
) }}

with high_transaction_amounts as (
    SELECT
        row_number() over() as risk_event_id,
        t.client_id,
        t.transaction_id,
        1 AS risk_reason_id,  -- 1 is the risk_reason_id for high transaction amounts
        100 AS risk_score,  -- Assign a risk score, e.g., 100
        t.transaction_date AS event_timestamp
    FROM
        {{ ref('transactions_silver_layer') }} t
    WHERE
        t.transaction_amount > 1000000
),

high_risk_countries as (
    SELECT
        row_number() over() + (SELECT count(*) FROM high_transaction_amounts) as risk_event_id,
        t.client_id,
        t.transaction_id,
        3 AS risk_reason_id,  -- 3 is the risk_reason_id for high-risk countries
        80 AS risk_score,  -- Assign a risk score, e.g., 80
        t.transaction_date AS event_timestamp
    FROM
        {{ ref('transactions_silver_layer') }} t
    JOIN
        {{ ref('clients_silver_layer') }} c ON t.client_id = c.client_id
    WHERE
        c.country IN ('IRAN', 'NORTH KOREA')  -- Assumed high-risk countries
    OR  
        t.country IN ('IRAN', 'NORTH KOREA')  -- Assumed high-risk countries
)

select *
from high_transaction_amounts

union all

select *
from high_risk_countries