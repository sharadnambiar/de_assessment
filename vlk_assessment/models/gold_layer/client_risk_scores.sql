{{ config(
    materialized='view',
    tags = 'gold_layer',enabled = true
        ) 
}}


with joined as (
    SELECT
    re.risk_event_id,
    re.client_id,
    c.name,
    c.city,
    c.country,
    c.contact_number,
    rf.risk_reason_id,
    rf.risk_reason,
    re.risk_score,
    re.event_timestamp 
    FROM
         {{ source('silver_schema', 'risk_event_silver_layer') }} re
    JOIN
         {{ source('silver_schema', 'risk_factor_silver_layer') }} rf ON re.risk_reason_id = rf.risk_reason_id
    JOIN
         {{ source('silver_schema', 'clients_silver_layer') }} c ON re.client_id = c.client_id

)

select
    risk_event_id,
    client_id,
    name,
    city,
    country,
    contact_number,
    risk_reason_id,
    risk_reason,
    risk_score,
    event_timestamp 
from joined
