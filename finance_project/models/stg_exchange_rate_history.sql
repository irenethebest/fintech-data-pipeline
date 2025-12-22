with source as (

    -- Select from the raw table defined in sources.yml
    select * from {{ source('fintech_bronze', 'exchange_rates_history') }}

),

renamed as (

    select
        -- IDS/KEYS
        -- We treat the combination of date + currency as the unique key here
        currency as target_currency,
        'USD' as base_currency, -- Hardcoding this since the column is 'rate_to_usd'

        -- VALUES
        -- Use 6 decimals for rates to handle smaller currencies (like JPY or IDR) accurately
        cast(rate_to_usd as decimal(10,6)) as exchange_rate,

        -- DATES
        -- Rename 'date' to 'effective_date' to avoid confusion with SQL reserved words
        cast(date as date) as effective_date,

        -- METADATA
        cast(time_processed as timestamp) as ingested_at

    from source

)

select * from renamed