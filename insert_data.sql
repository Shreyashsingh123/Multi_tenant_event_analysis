insert into analytics.tenant(tenant_id,tenant_name)
select
't' || i,
'tenant_' || i
from generate_series(1,100) as s(i);


insert into analytics.user(user_id,tenant_id)
select
'u' || i,
't'|| ((random()*99) :: int +1)
from generate_series(1,1000) as s(i);


INSERT INTO analytics.event (tenant_id, user_id, event_name, event_time, properties)
SELECT
    't' || (floor(random()*100) + 1)::int,
    'u' || (floor(random()*1000) + 1)::int,
    CASE
        WHEN random() < 0.25 THEN 'signup'
        WHEN random() < 0.5 THEN 'view'
        WHEN random() < 0.75 THEN 'add_to_cart'
        ELSE 'purchase'
    END,
    timestamp '2026-04-01' + (random() * interval '29 days'),
    jsonb_build_object(
        'amount', (random()*1000)::int,
        'device', CASE
            WHEN random() < 0.5 THEN 'mobile'
            ELSE 'desktop'
        END
    )
FROM generate_series(1, 10000);

select * from analytics.user