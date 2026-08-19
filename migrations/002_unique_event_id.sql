-- Replace the non-unique index with a UNIQUE constraint so that
-- INSERT ... ON CONFLICT (event_id) DO NOTHING works correctly.
DROP INDEX IF EXISTS idx_events_event_id;
ALTER TABLE events ADD CONSTRAINT uq_events_event_id UNIQUE (event_id);
