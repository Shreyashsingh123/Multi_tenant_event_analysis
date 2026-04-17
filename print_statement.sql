-- counique user per day per tenant
select tenant_id,count(distinct user_id),event_time
from analytics.event 
group by tenant_id,event_time