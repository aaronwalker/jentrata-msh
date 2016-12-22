DROP FUNCTION IF EXISTS jentrata_archive(do_delete BOOLEAN, start_time TIMESTAMP);
CREATE FUNCTION jentrata_archive(do_delete BOOLEAN, start_time TIMESTAMP = now()) returns TABLE (archived_count bigint) language plpgsql as $$
  DECLARE
    message_archived_count bigint;
    repository_archived_count bigint;
    inbox_archived_count bigint;
    message_delete_count bigint;
    repository_delete_count bigint;
    inbox_delete_count bigint;
  BEGIN

    IF (SELECT NOT EXISTS (
      SELECT 1
      FROM   information_schema.tables
      WHERE  table_schema = 'public'
      AND    table_name = 'message_archive'
    )) THEN
        CREATE TABLE message_archive (LIKE message INCLUDING ALL);
    END IF;

    IF (SELECT NOT EXISTS (
      SELECT 1
      FROM   information_schema.tables
      WHERE  table_schema = 'public'
      AND    table_name = 'repository_archive'
    )) THEN
        CREATE TABLE repository_archive (LIKE repository INCLUDING ALL);
    END IF;

    IF (SELECT NOT EXISTS (
      SELECT 1
      FROM   information_schema.tables
      WHERE  table_schema = 'public'
      AND    table_name = 'inbox_archive'
    )) THEN
        CREATE TABLE inbox_archive (LIKE inbox INCLUDING ALL);
    END IF;

    INSERT INTO message_archive
    SELECT * FROM message
    WHERE time_stamp < start_time - INTERVAL '6 months'
    AND message_id not in (select message_id from message_archive);
    GET DIAGNOSTICS message_archived_count = ROW_COUNT;
    raise notice 'messages archived: %', message_archived_count;

    INSERT INTO repository_archive
    SELECT repo.* FROM repository repo, message msg
    WHERE msg.time_stamp < start_time - INTERVAL '6 months'
    AND repo.message_id = msg.message_id
    AND repo.message_box = msg.message_box
    AND repo.message_id not in (select message_id from repository_archive);
    GET DIAGNOSTICS repository_archived_count = ROW_COUNT;
    raise notice 'repository archived: %', repository_archived_count;

    INSERT INTO inbox_archive
    SELECT * FROM inbox
    WHERE message_id IN (
      SELECT message_id FROM message
      WHERE time_stamp < start_time - INTERVAL '6 months'
    )
    AND message_id not in (select message_id from inbox_archive);
    GET DIAGNOSTICS inbox_archived_count = ROW_COUNT;
    raise notice 'inbox archived: %', inbox_archived_count;

    IF do_delete THEN
      DELETE FROM inbox
      WHERE message_id IN (
        SELECT message_id FROM message
        WHERE time_stamp < start_time - INTERVAL '6 months'
      );
      GET DIAGNOSTICS inbox_delete_count = ROW_COUNT;
      raise notice 'inbox deleted: %', inbox_delete_count;

      DELETE FROM repository
      WHERE message_id IN (
        SELECT message_id FROM message
        WHERE time_stamp < start_time - INTERVAL '6 months'
      );
      GET DIAGNOSTICS repository_delete_count = ROW_COUNT;
      raise notice 'inbox repository: %', repository_delete_count;

      DELETE FROM message
      WHERE time_stamp < start_time - INTERVAL '6 months';
      GET DIAGNOSTICS message_delete_count = ROW_COUNT;
      raise notice 'message deleted: %', message_delete_count;
    ELSE
      raise notice 'not deleting archive records';
    END IF;

    RETURN QUERY
    SELECT message_archived_count;

  end $$;

  SELECT * FROM jentrata_archive(true);
