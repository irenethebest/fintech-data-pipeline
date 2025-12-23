with source as (

    -- This pulls data from the source we defined in Step 1
    select * from {{ source('fintech_bronze', 'transactions_history') }}

),

renamed as (

    select
        transaction_id,
        customer_id,
        
        -- CASTING: Ensure data types are correct for analytics
        cast(transaction_date as timestamp) as transaction_at,
        cast(transaction_date as date) as transaction_date,
        
        -- LOGIC: Handle the channel. If null, label it 'Unknown' or keep null.
        coalesce(purchase_channel, 'N/A') as purchase_channel,
        coalesce(purchase_category, 'N/A') as purchase_category,
        cast(amount as decimal(10,2)) as amount,
        currency,
        transaction_type,
        status,
        
        -- METADATA: Keep track of when this row was loaded
        cast(time_processed as timestamp) as ingested_at

    from source

)

select * from renamed