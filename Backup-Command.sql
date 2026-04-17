-- pg_dump -U postgres -d Event-Analysis -f backup.dump

-- use your own path to save the backup file, for example:
-- pg_dump -U postgres -d Event_Analysis -f C:\Users\shrey\OneDrive\Desktop\Postgree\Backup\event.sql


-- pg_restore -U postgres -d Event-Analysis backup.dump