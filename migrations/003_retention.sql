-- migrations/003_retention.sql
BEGIN;

CREATE OR REPLACE FUNCTION rag.prune_old(days_to_keep int)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
  deleted_count bigint;
BEGIN
  DELETE FROM rag.documents
  WHERE created_at < now() - (days_to_keep || ' days')::interval;
  GET DIAGNOSTICS deleted_count = ROW_COUNT;
  RETURN deleted_count;
END $$;

COMMIT;
