ALTER TABLE trust_movement ADD COLUMN flags SMALLINT UNSIGNED NOT NULL DEFAULT 0;
UPDATE trust_movement SET flags = 1 WHERE CONCAT(event_type, planned_event_type) = 'DEPARTUREDEPARTURE';
UPDATE trust_movement SET flags = 2 WHERE CONCAT(event_type, planned_event_type) = 'ARRIVALARRIVAL';
UPDATE trust_movement SET flags = 3 WHERE CONCAT(event_type, planned_event_type) = 'ARRIVALDESTINATION';
UPDATE trust_movement SET flags = flags + 4 WHERE event_source = 'MANUAL';

UPDATE trust_movement SET flags = flags + 8  WHERE variation_status = 'ON TIME';
UPDATE trust_movement SET flags = flags + 16 WHERE variation_status = 'LATE';
UPDATE trust_movement SET flags = flags + 24 WHERE variation_status = 'OFF ROUTE';

UPDATE trust_movement SET flags = flags + 32 WHERE offroute_ind;
UPDATE trust_movement SET flags = flags + 64 WHERE train_terminated;

ALTER TABLE trust_movement DROP COLUMN event_type;
ALTER TABLE trust_movement DROP COLUMN planned_event_type;
ALTER TABLE trust_movement DROP COLUMN event_source;
ALTER TABLE trust_movement DROP COLUMN variation_status;
ALTER TABLE trust_movement DROP COLUMN offroute_ind;
ALTER TABLE trust_movement DROP COLUMN train_terminated;
ALTER TABLE trust_movement DROP COLUMN correction_ind;
