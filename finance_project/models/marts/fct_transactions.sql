{{ config(
    materialized='table',
    schema='gold'
) }}

with transactions as (

    select * from {{ ref('stg_transactions_history') }}

),

exchange_rates as (

    select * from {{ ref('stg_exchange_rate_history') }}

),

final as (

    select
        -- Transaction Details
        t.transaction_id,
        t.customer_id,
        t.transaction_at,
        t.transaction_date,
        
        -- Money Logic
        t.amount as original_amount,
        t.currency as original_currency,
        
        -- Exchange Rate Logic
        -- If currency is USD, rate is 1.0. Otherwise use the joined rate.
        -- If no rate found for a foreign currency, default to 0 (or null) to flag issues.
        case 
            when t.currency = 'USD' then 1.0
            else coalesce(e.exchange_rate, 0)
        end as exchange_rate,
        
        -- USD Calculation
        case 
            when t.currency = 'USD' then t.amount
            else cast(t.amount * coalesce(e.exchange_rate, 0) as decimal(10,2))
        end as amount_usd,
        
        -- Metadata & Categorization
        t.purchase_category,
        t.purchase_channel,
        t.transaction_type,
        t.status,
        t.ingested_at

    from transactions t
    
    -- Left Join ensures we never lose a transaction, even if the exchange rate is missing
    left join exchange_rates e 
        on t.currency = e.target_currency 
        and t.transaction_date = e.effective_date

)

select * from final