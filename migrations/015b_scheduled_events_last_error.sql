-- Add last_error column to scheduled_events for failed event error tracking
ALTER TABLE scheduled_events ADD COLUMN IF NOT EXISTS last_error TEXT;