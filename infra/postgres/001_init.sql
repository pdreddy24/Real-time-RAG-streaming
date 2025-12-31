CREATE TABLE IF NOT EXISTS wikimedia_edits (
  event_time      TIMESTAMPTZ,
  wiki            TEXT,
  title           TEXT,
  user_name       TEXT,
  bot             BOOLEAN,
  change_type     TEXT,
  comment         TEXT,
  rcid            BIGINT,
  oldlen          INT,
  newlen          INT,
  raw_json        TEXT
);
