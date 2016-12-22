CREATE OR REPLACE FUNCTION jentrata_inbound_trigger(start_time TIMESTAMP, end_time TIMESTAMP) returns TABLE (retrigger_count bigint) language plpgsql as $$
BEGIN
  DROP TABLE IF EXISTS inbound_retrigger;

  CREATE TABLE inbound_retrigger AS
    SELECT message_id FROM message
      where message_box = 'inbox'
      AND message_type = 'Order'
      AND time_stamp >= start_time
      AND time_stamp < end_time;

    RETURN QUERY
    SELECT count(message_id) FROM inbound_retrigger;

END $$;

CREATE OR REPLACE FUNCTION jentrata_inbound_resend(batch integer) returns TABLE (retrigger_count bigint) language plpgsql as $$
DECLARE
  triggered_count bigint;
  msg RECORD;
BEGIN
  triggered_count = 0;
  FOR msg IN SELECT message_id FROM inbound_retrigger limit batch
  LOOP
    raise notice 'triggering message: %', msg.message_id;
    DELETE FROM inbox where message_id = msg.message_id;

    UPDATE message SET status='PD' WHERE message_id = msg.message_id;

    DELETE FROM inbound_retrigger WHERE message_id = msg.message_id;

    triggered_count = triggered_count + 1;

  END LOOP;

  RETURN QUERY
  SELECT triggered_count;

END $$;

CREATE OR REPLACE FUNCTION jentrata_inbound_resend_batch(batch_size integer, delay integer) returns VOID  language plpgsql as $$
DECLARE
  total_to_resend bigint;
  no_batches bigint;
BEGIN
  SELECT INTO total_to_resend count(*) FROM inbound_retrigger;
  no_batches = total_to_resend / batch_size;
  IF no_batches = 0 THEN
	no_batches = 1;
  END IF;
  raise notice 'number of batches: %', no_batches;
  FOR counter IN 1..no_batches LOOP
    raise notice 'triggering batch: %', counter;
    PERFORM jentrata_inbound_resend(batch_size);
    PERFORM pg_sleep(delay);
  END LOOP;

END $$;
