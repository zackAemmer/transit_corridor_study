CREATE TABLE delay_occurrences_study (
	tripID integer,
	vehicleID integer,
	delayAmount integer,
	lat float,
	lon float,
	prevStop integer,
	nextStop integer,
	delayTime integer,
	collectedTime integer
);
CREATE TABLE active_trips_study (
	tripID integer,
	vehicleID integer,
	lat float,
	lon float,
	orientation integer,
	scheduleDeviation integer,
	totalTripDistance float,
	tripDistance float,
	closestStop integer,
	nextStop integer,
	locationTime integer,
	collectedTime integer
);
CREATE TABLE corridor_stops (
	stopid integer,
	corridorid integer,
	hubdist float
);
CREATE TABLE corridor_trips (
	routeid integer,
	tripid integer
);
CREATE INDEX tripid_idx ON active_trips_study (tripid);
CREATE INDEX loctime_idx ON active_trips_study (locationTime);
CREATE INDEX corridor_tripid_idx ON corridor_trips (tripid);

SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = 'aws_gtfs'
  AND pid <> pg_backend_pid();
  
SELECT * FROM corridor_trips;
EXPLAIN (SELECT * FROM active_trips_study WHERE tripid IN (SELECT tripid FROM corridor_trips));
EXPLAIN SELECT * FROM active_trips_study

