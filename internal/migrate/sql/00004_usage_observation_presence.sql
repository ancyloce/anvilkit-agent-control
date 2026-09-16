-- +goose Up
-- Metering (DD-02 §3, delivery.md P06 correction): a usage report carries
-- either explicit cumulative counters (zero counters state that the provider
-- metered nothing) or no counters at all (the sender does not know what was
-- metered). The counter columns are NOT NULL, so absence is recorded here:
-- false marks a report whose counters are placeholders that never enter the
-- running maximum, never settle the call and never release its exposure.
-- Every row written before this migration carried counters.
ALTER TABLE usage_observations ADD COLUMN usage_reported boolean NOT NULL DEFAULT true;

-- +goose Down
ALTER TABLE usage_observations DROP COLUMN usage_reported;
