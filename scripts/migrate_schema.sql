-- Safe migration helpers for existing deployments.
-- Run with: psql "$DATABASE_URL" -f scripts/migrate_schema.sql

ALTER TABLE IF EXISTS observations
  ADD COLUMN IF NOT EXISTS btc_reference_price DOUBLE PRECISION NOT NULL DEFAULT 0;

ALTER TABLE IF EXISTS observations
  ADD COLUMN IF NOT EXISTS data_source VARCHAR(255);

ALTER TABLE IF EXISTS aggregated_stats
  ADD COLUMN IF NOT EXISTS bucket_day DATE;

UPDATE aggregated_stats
SET bucket_day = COALESCE(bucket_day, DATE(updated_at))
WHERE bucket_day IS NULL;

ALTER TABLE IF EXISTS aggregated_stats
  ALTER COLUMN bucket_day SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'uq_aggregated_stats_bucket'
  ) THEN
    ALTER TABLE aggregated_stats
      ADD CONSTRAINT uq_aggregated_stats_bucket
      UNIQUE (bucket_day, price_bucket, time_bucket, btc_gap_bucket);
  END IF;
END $$;
