#!/bin/bash

require(DBI)
require(odbc)
require(paws)

"This script gets run at ~midnight every night. It scrapes the AWS RDS database
for the last 24 hours, calculates average speeds for each segment, and updates
the Dynamodb table.
The Dynamodb table can then be queried for all kcm routes in a geojson file with
speed histories."
main = function() {
  secure = readRDS("secure.rds")
  
  #Get time
  time = as.POSIXct(Sys.time())
  end_time = as.integer(time)
  start_time = as.integer(end_time - (24*60*60))
  
  #Query the last 24 hours of data from the RDS database 
  rds_db = dbConnect(odbc(), Driver="PostgreSQL Unicode", Server="kcm-gtfs-production.c3rbzvtfbrdb.us-east-2.rds.amazonaws.com", Database="kcm-gtfs", UID=secure[2], PWD=secure[3], Port=5432)
  query_text = paste0('SELECT * 
                    FROM active_trips_study 
                    WHERE collectedtime 
                    BETWEEN ', start_time, ' AND ', end_time,';')
  daily_results = dbGetQuery(rds_db, query_text)
  dbDisconnect(rds_db)

  #Get the latest GTFS route - trip data from the KCM FTP server
  download.file('http://metro.kingcounty.gov/GTFS/google_transit.zip', "google_transit.zip")
  unzip('google_transit.zip', 'trips.txt', exdir='google_transit')
  gtfs_trips = read.table("google_transit/trips.txt", header=TRUE, sep=',', stringsAsFactors=FALSE)
  gtfs_trips = gtfs_trips[,c('route_id', 'trip_id', 'trip_short_name')]
  
  #Preprocessing out duplicates
  daily_results = daily_results[!duplicated(daily_results[,c('tripid', 'locationtime')]),]
  daily_results = daily_results[order(daily_results$tripid, daily_results$locationtime),]
  
  #Offset tripdistance, locationtime, and tripids by 1
  prev_tripdistance = daily_results$tripdistance[-nrow(daily_results)]
  prev_tripdistance = append(prev_tripdistance, NA, after=0)
  daily_results$prev_tripdistance = prev_tripdistance
  rm(prev_tripdistance)
  prev_locationtime = daily_results$locationtime[-nrow(daily_results)]
  prev_locationtime = append(prev_locationtime, NA, after=0)
  daily_results$prev_locationtime = prev_locationtime
  rm(prev_locationtime)
  prev_tripid = daily_results$tripid[-nrow(daily_results)]
  prev_tripid = append(prev_tripid, NA, after=0)
  daily_results$prev_tripid = prev_tripid
  rm(prev_tripid)
  
  #Remove NA rows, and rows where tripid is different (last recorded location)
  daily_results = daily_results[complete.cases(daily_results),]
  daily_results = daily_results[daily_results$tripid == daily_results$prev_tripid,]
  
  #Calculate average speed between each location bus is tracked at
  daily_results$dist_diff = daily_results$tripdistance - daily_results$prev_tripdistance
  daily_results$timediff = daily_results$locationtime - daily_results$prev_locationtime
  daily_results$avg_speed_m_s = daily_results$dist_diff / daily_results$timediff
  
  #Remove rows where speed is below 0 and round to one decimal place
  daily_results = daily_results[daily_results$avg_speed_m_s >= 0,]
  daily_results$avg_speed_m_s = round(daily_results$avg_speed_m_s, 1)
  
  #Aggregate and join the trip speeds to their routes
  daily_results = merge(daily_results, gtfs_trips, by.x='tripid', by.y='trip_id')
  daily_results = daily_results[,c('route_id', 'trip_short_name', 'avg_speed_m_s')]
  daily_agg = aggregate(daily_results, by=list(daily_results$route_id, daily_results$trip_short_name), FUN=mean, na.rm=TRUE)
  daily_agg$Group.2[daily_agg$Group.2=='LOCAL'] = 'L'
  daily_agg$Group.2[daily_agg$Group.2=='EXPRESS'] = 'E'
  daily_agg$avg_speed_m_s = round(daily_agg$avg_speed_m_s, 1)
  
  #Set up the connection to the Dynamodb database
  Sys.setenv(
    AWS_ACCESS_KEY_ID = secure[4],
    AWS_SECRET_ACCESS_KEY = secure[5],
    AWS_REGION = "us-east-2"
  )
  dynamo_db = paws::dynamodb()
  
  #Update the speeds in the dynamoDB (this will be the daily speed)
  for (i in 1:nrow(daily_agg)) {
    dynamo_db$update_item(
      TableName='KCM_Bus_Routes',
      Key=list(
        route_id=list(
          N=daily_agg[i,1]
        ),
        local_express_code=list(
          S=daily_agg[i,2]
        )
      ),
      UpdateExpression='SET avg_speed = :speed, #hs = list_append(#hs, :vals)',
      ExpressionAttributeNames=list(
        `#hs`='historic_speeds'
      ),
      ExpressionAttributeValues=list(
        ':speed'=list(
          N=daily_agg[i,5]
        ),
        `:vals`=list(
          L=list(
            list(
              N=daily_agg[i,5]
            )
          )
        )
      )
    )
    Sys.sleep(0.3)
  }
  
  return(1)
}

#Program starts here
main()
