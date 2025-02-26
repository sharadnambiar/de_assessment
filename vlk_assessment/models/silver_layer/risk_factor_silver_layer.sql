{{ config(
    alias='risk_factor_silver_layer',
    materialized='table',
    tags = 'silver_layer'
        ) 
}}




select * from (
    values 
        (1, 'High Transaction Amount'),
        (2, 'High Frequency of Transactions'),
        (3, 'High Risk Country'),
        (4, 'New device usage')
) AS risk_factors(risk_reason_id, risk_reason)