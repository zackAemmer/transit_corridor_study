library(DBI)
library(odbc)
library(ggplot2)
library(sqldf)
library(lubridate)
library(rgdal)
secure = readRDS("secure.rds")


#### STOP METHOD Get trips in corridors using the stops in corridors ####
#Put the list of unique corridor stops from GIS into the database in order to query closest trips
#corridor_stops = read.csv("stops_in_corridors.csv",stringsAsFactors = FALSE)
#corridor_stops = data.frame(corridor_stops$stop_id,corridor_stops$HubName,corridor_stops$HubDist)
#names(corridor_stops) = c("stopid","corridorid","hubdist")
#gtfs_db = dbConnect(odbc(), Driver="PostgreSQL Unicode", Server="kcm-gtfs-production.c3rbzvtfbrdb.us-east-2.rds.amazonaws.com", Database="kcm-gtfs", UID=secure[2], PWD=secure[3], Port=5432)
#dbWriteTable(gtfs_db, "corridor_stops", corridor_stops, append=TRUE, row.names=FALSE)
#dbDisconnect(gtfs_db)

#Get trips from database that are within the corridors of the study (by closest stop)
#gtfs_db = dbConnect(odbc(), Driver="PostgreSQL Unicode", Server="kcm-gtfs-production.c3rbzvtfbrdb.us-east-2.rds.amazonaws.com", Database="kcm-gtfs", UID=secure[2], PWD=secure[3], Port=5432)
#queryText = "SELECT * FROM active_trips_study WHERE closeststop IN (SELECT stopid FROM corridor_stops);"
#trips = dbGetQuery(gtfs_db, queryText)
#dbDisconnect(gtfs_db)
#saveRDS(trips,"trips_study_stop.rds")
trips = readRDS("trips_study_stop.rds")

#Get the vehicle ids and time periods for each vehicle
trips$date = as.Date(as.POSIXct(trips$locationtime, tz="America/Los_Angeles", origin="1970-01-01"))
trips$collectedDate = as.Date(as.POSIXct(trips$collectedtime, tz="America/Los_Angeles", origin="1970-01-01"))

#Get rid of any trips where the API put a weird/mismatched timestamp on (-1364)
trips = trips[trips$date==trips$collectedDate,]

#Get the time during which each unique vehicle id appears in the corridor, for each unique day and tripid
all_trips = data.frame()
for (day in c(3:7)) {
  daily_trips = trips[day==substr(trips$date,10,11),]
  unique_trips = sqldf(
    "SELECT x.tripid, x.vehicleid, y.first_time, x.last_time, y.first_stop, x.last_stop, y.first_distance, x.last_distance
    FROM (SELECT tripid, MIN(locationtime) AS first_time, closeststop AS first_stop, tripdistance AS first_distance FROM daily_trips GROUP BY vehicleid, tripid) y
    JOIN (SELECT tripid, vehicleid, MAX(locationtime) AS last_time, closeststop AS last_stop, tripdistance AS last_distance FROM daily_trips GROUP BY vehicleid, tripid) x
    ON y.tripid = x.tripid;")
  
  unique_trips$total_time = unique_trips$last_time - unique_trips$first_time
  unique_trips$start = as.POSIXct(unique_trips$first_time, tz="America/Los_Angeles", origin="1970-01-01")
  unique_trips$finish = as.POSIXct(unique_trips$last_time, tz="America/Los_Angeles", origin="1970-01-01")
  all_trips = rbind(all_trips,unique_trips)
}

#Remove trips that had erroneous data causing impossible traversal times
all_trips = all_trips[complete.cases(all_trips),] #(-8)
all_trips = all_trips[all_trips$total_time != 0,] #(-1215)
all_trips = all_trips[all_trips$total_time < 80000,] #(-4)
all_trips = all_trips[all_trips$total_time > 10,] #(-11)

#Remove non-peak-hour trips
x = as.numeric(substr(all_trips$start,12,13))
all_trips = all_trips[x %in% c(6,7,8,15,16,17),] #6-9am, 3-6pm
rm(x)

#Save data for future analysis
saveRDS(all_trips,"all_trips_stop.RDS")



#### GIS METHOD Redo the steps to get trips in corridor using the Route ids ####
#Get list of tripids from GTFS that fall in the routeids of interest, upload to database
#gtfs = read.csv("trips.csv",stringsAsFactors = FALSE)
#corridor_routes = read.csv("routes_in_corridor.csv",stringsAsFactors = FALSE)
#corridor_routes = data.frame(corridor_routes$ROUTE_ID)
#names(corridor_routes) = c("routeid")
#corridor_trips = merge(corridor_routes,gtfs,by.x="routeid",by.y="route_id")
#corridor_trips = data.frame(corridor_trips$routeid,corridor_trips$trip_id)
#names(corridor_trips) = c("routeid","tripid")
#gtfs_db = dbConnect(odbc(), Driver="PostgreSQL Unicode", Server="kcm-gtfs-production.c3rbzvtfbrdb.us-east-2.rds.amazonaws.com", Database="kcm-gtfs", UID=secure[2], PWD=secure[3], Port=5432)
#dbWriteTable(gtfs_db, "corridor_trips", corridor_trips, append=TRUE, row.names=FALSE)
#dbDisconnect(gtfs_db)

#Get trips from database that are within the corridors of the study (by tripid)
#gtfs_db = dbConnect(odbc(), Driver="PostgreSQL Unicode", Server="kcm-gtfs-production.c3rbzvtfbrdb.us-east-2.rds.amazonaws.com", Database="kcm-gtfs", UID=secure[2], PWD=secure[3], Port=5432)
#queryText = "SELECT * FROM active_trips_study WHERE tripid IN (SELECT tripid FROM corridor_trips);"
#trips = dbGetQuery(gtfs_db, queryText)
#dbDisconnect(gtfs_db)
#saveRDS(trips,"trips_study_gis.rds")
trips = readRDS("trips_study_gis.rds")

#Get the vehicle ids and time periods for each vehicle
trips$date = as.Date(as.POSIXct(trips$locationtime, tz="America/Los_Angeles", origin="1970-01-01"))
trips$collectedDate = as.Date(as.POSIXct(trips$collectedtime, tz="America/Los_Angeles", origin="1970-01-01"))

#Get rid of any trips where the API put a weird/mismatched timestamp on (-1364)
trips = trips[trips$date==trips$collectedDate,]

#Write to file for GIS analysis
#write.csv(trips,"trip_data.csv",row.names = FALSE)

#Load clipped trips from GIS analysis
trips = read.csv("trip_corridor_data.csv",stringsAsFactors = FALSE)

#(Repeat from above) Get the time during which each unique vehicle id appears in the data, for each unique day and tripid
all_trips = data.frame()
for (day in c(3:7)) {
  daily_trips = trips[day==substr(trips$date,10,11),]
  unique_trips = sqldf("SELECT DISTINCT tripid, vehicleid, MAX(locationtime) AS last_time, MIN(locationtime) AS first_time FROM daily_trips GROUP BY vehicleid, tripid;")
  unique_trips$total_time = unique_trips$last_time - unique_trips$first_time
  unique_trips$start = as.POSIXct(unique_trips$first_time, tz="America/Los_Angeles", origin="1970-01-01")
  unique_trips$finish = as.POSIXct(unique_trips$last_time, tz="America/Los_Angeles", origin="1970-01-01")
  all_trips = rbind(all_trips,unique_trips)
}

#Remove coach ids that do not have cameras installed
coach_ids_with_video = read.csv("coach_ids_with_video.csv", stringsAsFactors = FALSE)
coach_ids_with_video = substr(coach_ids_with_video$Coach.Base,0,4)
coach_ids_with_video = as.integer(coach_ids_with_video)
coach_ids_with_video = na.omit(coach_ids_with_video)
all_trips = all_trips[all_trips$vehicleid %in% coach_ids_with_video,] # -> 16090
saveRDS(all_trips,"all_trips_gis.RDS")

#clean up
rm(all_trips,daily_trips,trips,unique_trips,day)



#### Compare the two methods of clipping trips to corridors ####
gis_method_data = readRDS("all_trips_gis.RDS")
stop_method_data = readRDS("all_trips_stop.RDS")

#Trip data cleaning based on time and completeness
gis_method_data = gis_method_data[complete.cases(gis_method_data),] #(-8)
gis_method_data = gis_method_data[gis_method_data$total_time != 0,] #(-1215)
gis_method_data = gis_method_data[gis_method_data$total_time < 80000,] #(-4)
#gis_method_data = gis_method_data[gis_method_data$total_time > 10,] #(-11)

#Trip data cleaning based on time and completeness
stop_method_data = stop_method_data[complete.cases(stop_method_data),] #(-8)
stop_method_data = stop_method_data[stop_method_data$total_time != 0,] #(-1215)
stop_method_data = stop_method_data[stop_method_data$total_time < 80000,] #(-4)
#stop_method_data = stop_method_data[stop_method_data$total_time > 10,] #(-11)

#Summary statistics
par(mfrow=c(2,1))
hist(stop_method_data$total_time,breaks=100,ylim=c(0,2500),main="Stop Method Traversal Times",xlab="Secs")
hist(gis_method_data$total_time,breaks=100,ylim=c(0,2500),main="GIS Method Traversal Times",xlab="Secs")
summary(stop_method_data$total_time)
summary(gis_method_data$total_time)
total_hrs_trip = sum(gis_method_data$total_time)/60/60
total_hrs_stop = sum(stop_method_data$total_time)/60/60
tripids_trip = sqldf("SELECT DISTINCT tripid FROM gis_method_data;")
tripids_stop = sqldf("SELECT DISTINCT tripid FROM stop_method_data;")




#### Round dates then save data for email/logistics to KCM ####
all_trips = readRDS("all_trips_stop.RDS")

#Filter to only 6 peak hours (6-9, 4-7)
all_trips = all_trips[hour(all_trips$start) %in% c(6,7,8,16,17,18),]

#Remove coach ids that do not have cameras installed
coach_ids_with_video = read.csv("coach_ids_with_video.csv", stringsAsFactors = FALSE)
coach_ids_with_video = substr(coach_ids_with_video$Coach.Base,0,4)
coach_ids_with_video = as.integer(coach_ids_with_video)
coach_ids_with_video = na.omit(coach_ids_with_video)
all_trips = all_trips[all_trips$vehicleid %in% coach_ids_with_video,] #6434 -> 6296

#Round the start and finish times to selected value
all_trips$start_rounded = floor_date(all_trips$start, unit="5 minutes")
all_trips$finish_rounded = ceiling_date(all_trips$finish, unit="5 minutes")
all_trips = all_trips[order(all_trips$vehicleid, all_trips$start_rounded),]

#Get total amount of video data being requested (this is not actual amount of time buses are in corridor)
total_hrs_rounded = difftime(all_trips$finish_rounded, all_trips$start_rounded, units="hours")
total_hrs_rounded = sum(as.numeric(total_hrs_rounded))

#Reduce to 200 hrs of data (per week) (2:1 for 1600hrs of RA work) and save for analysis
RA_hours = 200
set.seed(924)
all_trips_sampled = all_trips[sample(nrow(all_trips), as.integer(RA_hours/total_hrs_rounded*nrow(all_trips))),]
saveRDS(all_trips_sampled, "all_trips_stop_sampled.RDS")

#Get total time for the reduced request
sample_hrs_rounded = difftime(all_trips_sampled$finish_rounded, all_trips_sampled$start_rounded, units="hours")
sample_hrs_rounded = sum(as.numeric(sample_hrs_rounded))

#Reduce to only necessary columns for the request, write csv for the request
all_trips_sampled = data.frame(all_trips_sampled$vehicleid, substr(as.character(all_trips_sampled$start_rounded),0,10), substr(as.character(all_trips_sampled$start_rounded),12,19), substr(as.character(all_trips_sampled$finish_rounded),12,19), all_trips_sampled$tripid)
names(all_trips_sampled) = c("vehicleid","date","start_rounded","finish_rounded","tripid")
all_trips_sampled = all_trips_sampled[order(all_trips_sampled$vehicleid, all_trips_sampled$date, all_trips_sampled$start_rounded, all_trips_sampled$finish_rounded),]
write.csv(all_trips_sampled, "video_request_5min.csv",row.names=FALSE)



#### Analyze the trip data that falls in corridors as determined by stop method ####
all_trips = readRDS("all_trips_stop.RDS")
all_trips_sampled = readRDS("all_trips_stop_sampled.RDS")

all_trips = all_trips_sampled

corridor_distances = c(3009,1603,2191,1710,1549,1863,2360,3708,1988,1373)
corridor_stops = read.csv("stops_in_corridors.csv",stringsAsFactors = FALSE)

#Quantify and map the speed/reliability/travel time of transit at segment level
all_trips$corridor_id=10
all_trips$corridor_distance = 0
all_trips$distance = 0
all_trips$speed=0
for (id in 0:9) {
  current_stops = corridor_stops[corridor_stops$HubName == id,]
  all_trips[all_trips$first_stop %in% current_stops$stop_id,]$corridor_id = id
  all_trips[all_trips$first_stop %in% current_stops$stop_id,]$corridor_distance = corridor_distances[id+1]/5280
}
rm(id,corridor_distances)
all_trips$distance = (all_trips$last_distance - all_trips$first_distance)*3.28084/5280
all_trips$speed = all_trips$distance/(all_trips$total_time/60/60)
all_trips = all_trips[all_trips$speed > 0,]

#Get the 95th percentile speed (freeflow), average speed, and systematic delay (ff-avg/length) for each corridor separately
all_trips$ff_speed=0
all_trips$avg_speed=0
all_trips$systematic_delay=0
all_trips$stochastic_delay=0

#Returns the percentile value of a vector containing a distribution of numeric values
getPercentile = function(percentile, distribution) {
  avg = mean(distribution)
  sd = sd(distribution)
  z = qnorm(percentile)
  ff = avg + z*sd
  return(ff)
}

for (id in 0:9) {
  current_trips = all_trips[all_trips$corridor_id==id,]
  all_trips[all_trips$corridor_id==id,]$ff_speed = getPercentile(0.95, current_trips$speed)
  all_trips[all_trips$corridor_id==id,]$avg_speed = mean(current_trips$speed)
}

#Systematic: difference between average speed and freeflow speed
#Stochastic: difference between systematic and actual speed
all_trips$systematic_delay = ((all_trips$corridor_distance/all_trips$avg_speed) - (all_trips$corridor_distance/all_trips$ff_speed)) *60*60
all_trips$stochastic_delay = (all_trips$corridor_distance/all_trips$speed*60*60) - all_trips$systematic_delay

#Display the distributions of speed in mph for each of the study corridors separately
par(mfrow=c(2,1))
for (id in 0:1) {
  current_trips = all_trips[all_trips$corridor_id==id,]
  hist(current_trips$speed,breaks=100,ylim=c(0,100),xlim=c(0,25),main=paste("Corridor Speeds",as.character(id)),xlab="MPH")
}
for (id in 2:3) {
  current_trips = all_trips[all_trips$corridor_id==id,]
  hist(current_trips$speed,breaks=100,ylim=c(0,100),xlim=c(0,25),main=paste("Corridor Speeds",as.character(id)),xlab="MPH")
}
for (id in 4:5) {
  current_trips = all_trips[all_trips$corridor_id==id,]
  hist(current_trips$speed,breaks=100,ylim=c(0,100),xlim=c(0,25),main=paste("Corridor Speeds",as.character(id)),xlab="MPH")
}
for (id in 6:7) {
  current_trips = all_trips[all_trips$corridor_id==id,]
  hist(current_trips$speed,breaks=100,ylim=c(0,100),xlim=c(0,25),main=paste("Corridor Speeds",as.character(id)),xlab="MPH")
}
for (id in 8:9) {
  current_trips = all_trips[all_trips$corridor_id==id,]
  hist(current_trips$speed,breaks=100,ylim=c(0,100),xlim=c(0,25),main=paste("Corridor Speeds",as.character(id)),xlab="MPH")
}

#Export to google sheet and draw comparisons between the pre-post sampled corridors
for (id in 0:9) {
  metric = sum(all_trips[all_trips$corridor_id==id,]$total_time)/60/60
  print(paste(id, ": ", metric))
}
