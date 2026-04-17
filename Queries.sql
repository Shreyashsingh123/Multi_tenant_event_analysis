-- count unique user per day per tenant
select tenant_id,count(distinct user_id),event_time
from analytics.event 
group by tenant_id,event_time

-- funnel analysis
select
count (distinct case when event_name='signup' then user_id end) as signup,
count (distinct case when event_name='add_to_cart' then user_id end) as cart,
count (distinct case when event_name='purchase' then user_id end) as purchase
from analytics.event;

-- retention analysis
WITH first_day AS (
    SELECT user_id, MIN(DATE(event_time)) AS signup_day
    FROM analytics.event
    GROUP BY user_id
),
return_day AS (
    SELECT e.user_id
    FROM analytics.event e 
    JOIN first_day f ON e.user_id = f.user_id
    WHERE DATE(e.event_time) = f.signup_day + 1
)
SELECT COUNT(*) FROM return_day;

-- Advance query

-- top 5 most active user per tenants
with user_count as(
	select 
		tenant_id,
		user_id,
		count(*)  as event_count,
		row_number() over (
		partition by tenant_id
		order by count(*) desc
		) as rn
	from analytics.event 
	group by tenant_id,user_id
	
)
select tenant_id,user_id,event_count 
from user_count
where rn<=5

-- Event distribution per tenant by event type
with event_count as(
	select 
		tenant_id,
		count (distinct case when event_name='signup' then user_id end) as signup,
		count (distinct case when event_name='add_to_cart' then user_id end) as cart,
		count (distinct case when event_name='purchase' then user_id end) as purchase
		from analytics.event
		group by tenant_id
)
select * from event_count
order by tenant_id asc

-- Total revenue per tenant (from JSONB field)

with total_revenue as(
select tenant_id,
sum((properties->>'amount')::int) as s
from analytics.event
where event_name='purchase'
group by tenant_id
)
select * from total_revenue 

-- Users with no activity (LEFT JOIN + NULL filtering)
select u.user_id,u.tenant_id 
from analytics.user u
left join analytics.event e
on u.user_id=e.user_id
and u.tenant_id=e.tenant_id
where e.user_id is null

-- identify user associated with multiple tenants
select user_id,count(distinct(tenant_id)) as cs
from analytics.event
group by user_id
having count(distinct(tenant_id)) >1;

-- First event per user using ROW_NUMBER
select * from(
select user_id,tenant_id,event_name,event_time,
row_number() over(partition by user_id 
order by (event_time) asc) as r
from analytics.event
)
where r=1

-- Detect session gaps using LAG
SELECT
    user_id,
    event_time,
    LAG(event_time) OVER (
        PARTITION BY user_id
        ORDER BY event_time
    ) AS prev_event_time,
    
    event_time - LAG(event_time) OVER (
        PARTITION BY user_id
        ORDER BY event_time
    ) AS gap,
    
    CASE 
        WHEN event_time - LAG(event_time) OVER (
            PARTITION BY user_id
            ORDER BY event_time
        ) > INTERVAL '30 minutes'
        THEN 'NEW SESSION'
        ELSE 'SAME SESSION'
    END AS session_flag

FROM analytics.event;

--Running total of events per user

SELECT
    user_id,
    event_time,
    COUNT(*) OVER (
        PARTITION BY user_id
        ORDER BY event_time
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM analytics.event;


SELECT user_id, COUNT(*) AS event_count
FROM analytics.event
GROUP BY user_id
HAVING COUNT(*) > (
    SELECT AVG(user_event_count)
    FROM (
        SELECT COUNT(*) AS user_event_count
        FROM analytics.event
        GROUP BY user_id
    ) sub
);

SELECT *
FROM analytics.event e1
WHERE event_time = (
    SELECT MAX(e2.event_time)
    FROM analytics.event e2
    WHERE e2.user_id = e1.user_id
);

-- funnel analysis 

WITH signup AS (
    SELECT DISTINCT user_id
    FROM analytics.event
    WHERE event_name = 'signup'
),
cart AS (
    SELECT DISTINCT user_id
    FROM analytics.event
    WHERE event_name = 'add_to_cart'
),
purchase AS (
    SELECT DISTINCT user_id
    FROM analytics.event
    WHERE event_name = 'purchase'
)

SELECT
    (SELECT COUNT(*) FROM signup) AS signup_users,
    (SELECT COUNT(*) FROM cart) AS cart_users,
    (SELECT COUNT(*) FROM purchase) AS purchase_users;


WITH first_day AS (
    SELECT user_id, MIN(DATE(event_time)) AS signup_day
    FROM analytics.event
    GROUP BY user_id
),
return_day AS (
    SELECT DISTINCT e.user_id
    FROM analytics.event e
    JOIN first_day f ON e.user_id = f.user_id
    WHERE DATE(e.event_time) = f.signup_day + 1
)

SELECT 
    COUNT(DISTINCT f.user_id) AS total_users,
    COUNT(DISTINCT r.user_id) AS retained_users,
    (COUNT(DISTINCT r.user_id) * 100.0 / COUNT(DISTINCT f.user_id)) AS retention_rate
FROM first_day f
LEFT JOIN return_day r ON f.user_id = r.user_id;


WITH tenant_daily AS (
    SELECT 
        tenant_id,
        DATE(event_time) AS event_date,
        COUNT(*) AS daily_events
    FROM analytics.event
    GROUP BY tenant_id, DATE(event_time)
),
tenant_total AS (
    SELECT 
        tenant_id,
        SUM(daily_events) AS total_events
    FROM tenant_daily
    GROUP BY tenant_id
)

SELECT *
FROM tenant_total
ORDER BY total_events DESC
LIMIT 5;

--Combine CTE + window function to rank users within each tenant
WITH user_activity AS (
    SELECT 
        tenant_id,
        user_id,
        COUNT(*) AS total_events
    FROM analytics.event
    GROUP BY tenant_id, user_id
),
ranked_users AS (
    SELECT 
        tenant_id,
        user_id,
        total_events,
        RANK() OVER (
            PARTITION BY tenant_id
            ORDER BY total_events DESC
        ) AS rank
    FROM user_activity
)

SELECT *
FROM ranked_users
WHERE rank <= 5;

--Use subquery + JOIN to filter high-value users
SELECT u.user_id, u.total_spent
FROM (
    -- Step 1: Calculate total spending per user
    SELECT 
        user_id,
        SUM((properties->>'amount')::int) AS total_spent
    FROM analytics.event
    WHERE event_name = 'purchase'
    GROUP BY user_id
) u
JOIN (
    -- Step 2: Find average spending
    SELECT 
        AVG(total_spent) AS avg_spent
    FROM (
        SELECT 
            user_id,
            SUM((properties->>'amount')::int) AS total_spent
        FROM analytics.event
        WHERE event_name = 'purchase'
        GROUP BY user_id
    ) sub
) avg_table
ON u.total_spent > avg_table.avg_spent;

--Demonstrate partition pruning using EXPLAIN ANALYZE
EXPLAIN ANALYZE
SELECT *
FROM analytics.event
WHERE tenant_id='1'
AND event_time BETWEEN '2026-04-01' AND '2026-04-30';

