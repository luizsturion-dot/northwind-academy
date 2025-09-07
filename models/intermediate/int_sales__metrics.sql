with
    -- import models
    order_items as (
        select *
        from {{ ref('stg_erp__order_items') }}
    )
    , orders as (
        select *
        from {{ ref('stg_erp__orders') }}
    )
        -- Esta parte de cima podemos puxar (copiar e colar) o cóodigo de outras tabelas e só alterar o nome e a ref

    -- transformation
    , joined as (
        select
            order_items.order_item_sk
            , order_items.product_fk
            , orders.employee_fk
            , orders.customer_fk
            , orders.shipper_fk
            , orders.order_date
            , orders.ship_date
            , orders.required_delivery_date
            , order_items.discount_pct
            , order_items.unit_price
            , order_items.quantity
            , orders.freight
            , orders.order_number
            , orders.recipient_name
            , orders.recipient_city
            , orders.recipient_region
            , orders.recipient_country
        from order_items
        inner join orders on order_items.order_fk = orders.order_pk
            -- Aqui normalmente começamos definindo o from, depois escrevendo o join e só depois puxando no select.
            -- Boa prática: usar a tabela maior no from e a menor no left join (nesse caso foi inner join).
            -- No ordem das colunas, a boa prática: chave primaria é a primeira coluna, chaves secundárias em seguida, depois datas do fato,
            -- depois colunas de métrica e por último as categóricas.
    )

    -- Antes de começar as métricas, é boa prática testar se está ok. Então usamos um select* joined ao final,
    -- testamos o código com dbt run (dbr run --select staging.erp) este comando roda a pasta toda. Seestiver ok continuamos.

    , metrics as (
        select
            order_item_sk
            , product_fk
            , employee_fk
            , customer_fk
            , shipper_fk
            , order_date
            , ship_date
            , required_delivery_date
            , discount_pct
            , unit_price
            , quantity
            , unit_price * quantity as gross_total
                -- total bruto (gross_total) é o preço da unidade * a quantidade
            , unit_price * (1 - discount_pct) * quantity as net_total
                -- total líquido (net_total) está multiplicando a quantidade pelo valor retirado o desconto (1 - discount_pct)
            , cast(
                (freight / count(*) over (partition by order_number))
            as numeric(18,2)) as freight_allocated
                -- Aqui precisamos criar uma janela (over), que pega um intervalo de linhas (p/ ter o total de produtos por nota fiscal).
                -- Ai ele conta (count) o número de linhas da janela e divide o frete por esse número.
            , case
                when discount_pct > 0 then true
                else false
            end as had_discount
                -- had_discount ve o que teve desconto ou não.
            , order_number
            , recipient_name
            , recipient_city
            , recipient_region
            , recipient_country
        from joined
    )

select *
from metrics