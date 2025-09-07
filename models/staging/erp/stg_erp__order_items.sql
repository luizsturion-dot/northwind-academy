with
    source_order_details as (
        select *
        from {{ source('erp', 'order_details') }}
    )

    , renamed as (
        select
            {{ dbt_utils.generate_surrogate_key(['orderid', 'productid']) }} as order_item_sk
            -- Aqui ele está usando o dbt utils para gerar uma chave surrogada, ou seja, 
            -- uma chave que combina as chaves das duas colunas criando uma hash única.
            -- Tem outras maneiras de fazer isso, mas esta é a boa prática.
            , cast(orderid as int) as order_fk
            , cast(productid as int) as product_fk
            , cast(discount as numeric(18,2)) as discount_pct
            , cast(unitprice as numeric(18,2)) as unit_price
            , cast(quantity as int) as quantity
        from source_order_details
    )

select *
from renamed