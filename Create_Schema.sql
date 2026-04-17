create schema analytics
create table analytics.tenant(
	tenant_id Text primary key,
	tenant_name Text Not Null,
	status Text default 'active',
	created_at Timestamp Default Now(),
	updated_at Timestamp Default Now()
);

create table analytics.User(
user_id Text,
tenant_id Text,
created_at Timestamp Default Now(),
primary key(user_id,tenant_id),
Foreign key (tenant_id) References analytics.tenant(tenant_id) on delete cascade
);

create table analytics.event(
	id Bigserial,
	tenant_id Text not null,
	user_id text,
	event_name text not null,
	event_time Timestamp not null,
	properties jsonb,
	created_at timestamp default now(),
	primary key(tenant_id,id,event_time),
	foreign key(tenant_id) references analytics.tenant(tenant_id) on delete cascade 
)partition by range(event_time);

CREATE TABLE events_2026_04 PARTITION OF analytics.event
FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');


CREATE TABLE events_2026_05 PARTITION OF analytics.event
FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

CREATE TABLE events_2026_06 PARTITION OF analytics.event
FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

CREATE TABLE events_2026_07 PARTITION OF analytics.event
FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');

create index idx_tenant_time on analytics.event(tenant_id,event_time desc);
create index idx_event_name on analytics.event(event_name);
create index idx_properties_gin on analytics.event(properties)

Alter table analytics.event add column event_id text;
CREATE UNIQUE INDEX uniq_event
ON analytics.event (tenant_id, event_id,event_time);


CREATE OR REPLACE FUNCTION set_created_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.created_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_created_at
BEFORE INSERT ON analytics.event
FOR EACH ROW
EXECUTE FUNCTION set_created_at();

-- check event should not be null

CREATE OR REPLACE FUNCTION validate_event()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.event_name IS NULL THEN
        RAISE EXCEPTION 'event_name cannot be null';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_validate_event
BEFORE INSERT ON analytics.event
FOR EACH ROW
EXECUTE FUNCTION validate_event();

-- stored procedure 

CREATE OR REPLACE FUNCTION bulk_insert_events(events JSONB)
RETURNS VOID AS $$
DECLARE
    rec JSONB;
BEGIN
    FOR rec IN SELECT * FROM jsonb_array_elements(events)
    LOOP
        INSERT INTO analytics.event (tenant_id, user_id, event_name, event_time, properties)
        VALUES (
            rec->>'tenant_id',
            rec->>'user_id',
            rec->>'event_name',
            (rec->>'event_time')::timestamp,
            rec->'properties'
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION delete_old_events()
RETURNS VOID AS $$
BEGIN
    DELETE FROM analytics.event
    WHERE event_time < NOW() - INTERVAL '90 days';
END;
$$ LANGUAGE plpgsql;

-- materialised view for fastre access

-- distinct user 
create materialized view analytics.unique_user_mv as 
select 
tenant_id,date(event_time) as even_date,
count (distinct user_id) as dau
from analytics.event
group by tenant_id,date(event_time)

create materialized view analytics.revenue_mv as
select 
tenant_id,date(event_time) as event_date,
sum((properties->>'amount')::int) as revenue
from analytics.event
where event_name='purchase'
group by tenant_id,date(event_time)


create unique index dav_mv_idx
on analytics.unique_user_mv(tenant_id,even_date)


create unique index revenue_mv_idx
on analytics.revenue_mv (tenant_id,event_date)


REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.unique_user_mv;
REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.revenue_mv;
**

