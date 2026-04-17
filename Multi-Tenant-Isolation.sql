ALTER TABLE analytics.event ENABLE ROW LEVEL SECURITY;

ALTER TABLE analytics.event FORCE ROW LEVEL SECURITY;


CREATE POLICY tenant_isolation_policy
ON analytics.event
FOR ALL
USING (tenant_id = current_setting('app.current_tenant'));

SET app.current_tenant = 't1';

SELECT * FROM analytics.event;


SET app.current_tenant = 't2';

SELECT DISTINCT tenant_id FROM analytics.event;