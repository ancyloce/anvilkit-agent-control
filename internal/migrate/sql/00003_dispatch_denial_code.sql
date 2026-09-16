-- +goose Up
-- Single-use dispatch admission (DD-02 §4, delivery.md P06): a refused
-- admission is recorded under the call identity in state 'denied' so a
-- repeated request returns the same refusal and never a permission. The
-- public code of that refusal (contracts.md §4 vocabulary) is kept with the
-- row; rows that were never denied carry NULL.
ALTER TABLE dispatches ADD COLUMN denial_code text;

-- +goose Down
ALTER TABLE dispatches DROP COLUMN denial_code;
