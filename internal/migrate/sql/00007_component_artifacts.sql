-- +goose Up
-- P10 (DD-04 §2–§3, contracts/proto/anvilkit/control/v1/artifact.proto): the
-- build deliverables of a certified component are artifact classes of
-- their own — the npm tarball, the browser ES module and a stylesheet —
-- so a validator stage binds them as finalized transfers by exact digest
-- and size, never disguised as another class. Only the class checks
-- widen; every existing row and constraint keeps its meaning.

ALTER TABLE artifact_transfers
    DROP CONSTRAINT artifact_transfers_class_check,
    ADD CONSTRAINT artifact_transfers_class_check
        CHECK (class IN ('prompt', 'brief', 'source', 'stage', 'result', 'evidence', 'answer', 'argument', 'npm', 'browser', 'css'));

ALTER TABLE stage_artifacts
    DROP CONSTRAINT stage_artifacts_class_check,
    ADD CONSTRAINT stage_artifacts_class_check
        CHECK (class IN ('prompt', 'brief', 'source', 'stage', 'result', 'evidence', 'answer', 'argument', 'npm', 'browser', 'css'));

-- +goose Down
ALTER TABLE stage_artifacts
    DROP CONSTRAINT stage_artifacts_class_check,
    ADD CONSTRAINT stage_artifacts_class_check
        CHECK (class IN ('prompt', 'brief', 'source', 'stage', 'result', 'evidence', 'answer', 'argument'));
ALTER TABLE artifact_transfers
    DROP CONSTRAINT artifact_transfers_class_check,
    ADD CONSTRAINT artifact_transfers_class_check
        CHECK (class IN ('prompt', 'brief', 'source', 'stage', 'result', 'evidence', 'answer', 'argument'));
