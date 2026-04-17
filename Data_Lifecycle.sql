DROP TABLE IF EXISTS events_2026_01;

CREATE TABLE IF NOT EXISTS events_archive (
    LIKE analytics.event INCLUDING ALL
);

INSERT INTO events_archive
SELECT *
FROM analytics.event
WHERE event_time < '2026-05-01';

DELETE FROM analytics.event
WHERE event_time < '2026-03-01';

CREATE OR REPLACE FUNCTION archive_and_cleanup(p_cutoff_date TIMESTAMP)
RETURNS VOID AS $$
BEGIn
    INSERT INTO events_archive
    SELECT *
    FROM analytics.event
    WHERE event_time < p_cutoff_date;
	
    DELETE FROM analytics.event
    WHERE event_time < p_cutoff_date;

    RAISE NOTICE 'Archival and cleanup completed';
END;
$$ LANGUAGE plpgsql;

SELECT archive_and_cleanup('2026-05-01');