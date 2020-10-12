require(DBI)
require(odbc)
require(httr)
require(jsonlite)

#makes a call to the specified api, parses the json and returns a dataframe
#requires an api key, and returns invalid if 200 code not received
scrapeAPI = function(key,endpoint) {

  call = paste0(endpoint,key)
  currentResponse = GET(call)
  
  if (currentResponse$status_code == 200) {
    currentResponseText = content(currentResponse)
    currentResponseText = currentResponseText$data$list
    return(currentResponseText)
  } else {
    return(currentResponse)
  }
  print("something wrong")
  return()
}

#filter a list of lists containing vehicle information to get only active vehicles
getActiveVehicles = function(list) {
  
  #iterate through the list of all active vehicles in the network
  #where a vehicle has ben assigned to an active trip and has been given a location update, include it in the result
  tripStatusList = c()
  for (i in 1:length(list)) {
    if (list[[i]]$tripId != "" && list[[i]]$status != "CANCELED" && !is.null(list[[i]]$location)) {
      tripStatusList = c(tripStatusList,list(list[[i]]))
    }
  }
  return(tripStatusList)
}

#select desired location and delay information from dataframe containing lists of vehicle information
getDelayInfo = function(df_result) {
  
  final = c()
  for (i in 1:length(df_result$X1)) {
    current = c(df_result$X6[[i]], df_result$X8[[i]], df_result$X3[[i]]$lat, df_result$X3[[i]]$lon, df_result$X7[[i]]$orientation, df_result$X7[[i]]$scheduleDeviation, df_result$X7[[i]]$totalDistanceAlongTrip, df_result$X7[[i]]$distanceAlongTrip, df_result$X7[[i]]$closestStop, df_result$X7[[i]]$nextStop, df_result$X7[[i]]$lastLocationUpdateTime)
    final = rbind(final,current)
  }
  return(final)
}

#function to scrape data from the OBA API
getTrips = function(key) {
  
  #get a list of all vehicle ids active for KCM
  result = scrapeAPI(key,"http://api.pugetsound.onebusaway.org/api/where/vehicles-for-agency/1.json?key=")
  active_result = getActiveVehicles(result)
  
  #convert list of elements to a dataframe
  df_result = data.frame(matrix(unlist(active_result,recursive=FALSE), nrow=length(active_result), byrow=TRUE))
  
  #get the tripId, vehicleId, location, and current delay (and others) for each active vehicle
  final = as.data.frame(getDelayInfo(df_result),stringsAsFactors = FALSE)
  names(final) = c("tripID","vehicleID","lat","lon","orientation","scheduleDeviation","totalTripDistance","tripDistance","closestStop","nextStop","dateTime")
  
  return(final)
}

#record all active trips into the database during the time specified at frequency specified
recordTrips = function(frequency,gtfs_db,key) {
  
  #iterate and track locations of delay for the specified time of day
  time = as.POSIXct(Sys.time())
  hour = format(time, tz="America/Los_Angeles",usetz=TRUE)
  hour = as.numeric(substr(hour,12,13))
  
  while (hour < 19) {
    #get active trips and time immediately after for best accuracy
    currentSystemTrips = getTrips(key)
    time = as.POSIXct(Sys.time())
    hour = format(time, tz="America/Los_Angeles",usetz=TRUE)
    hour = as.numeric(substr(hour,12,13))
    
    #remove trailing zeroes on date, add the current collection time
    currentSystemTrips$dateTime = substr(currentSystemTrips$dateTime,1,10)
    currentSystemTrips['collectedTime'] = as.integer(time)
    names(currentSystemTrips) = c("tripid","vehicleid","lat","lon","orientation","scheduledeviation","totaltripdistance","tripdistance","closeststop","nextstop","locationtime","collectedtime")
    
    #truncate agency codes, put all variables in correct type
    currentSystemTrips$tripid = as.integer(sapply(currentSystemTrips$tripid,remove_agency_tag))
    currentSystemTrips$vehicleid = as.integer(sapply(currentSystemTrips$vehicleid,remove_agency_tag))
    currentSystemTrips$closeststop = as.integer(sapply(currentSystemTrips$closeststop,remove_agency_tag))
    currentSystemTrips$nextstop = as.integer(sapply(currentSystemTrips$nextstop,remove_agency_tag))
    currentSystemTrips$scheduledeviation = as.integer(currentSystemTrips$scheduledeviation)
    currentSystemTrips$totaltripdistance = as.numeric(currentSystemTrips$totaltripdistance)
    currentSystemTrips$tripdistance = as.numeric(currentSystemTrips$tripdistance)
    currentSystemTrips$locationtime = as.integer(currentSystemTrips$locationtime)
    currentSystemTrips$lat = as.numeric(currentSystemTrips$lat)
    currentSystemTrips$lon = as.numeric(currentSystemTrips$lon)
    currentSystemTrips$orientation = as.integer(currentSystemTrips$orientation)
    
    #post to database
    dbWriteTable(gtfs_db, "active_trips_study", currentSystemTrips, append=TRUE, row.names=FALSE)
    
    #wait number of seconds specified
    Sys.sleep(frequency)
  }
  return()
}

#Removes agency identification from ids and converts to numeric
remove_agency_tag = function(id) {
  final = substr(id,3,nchar(id))
  return(final)
}

#each iteration includes another data point for all routes and increases duration by 10 seconds
main = function(frequency,key,db_uid,db_pwd) {

  #connect to the aws-gtfs database using odbc manually with the Postgresql driver
  gtfs_db = dbConnect(odbc(), Driver="PostgreSQL Unicode", Server="kcm-gtfs-production.c3rbzvtfbrdb.us-east-2.rds.amazonaws.com", Database="kcm-gtfs", UID=db_uid, PWD=db_pwd, Port=5432)
  recordTrips(frequency,gtfs_db,key)
  
  dbDisconnect(gtfs_db)
  return(1)
}

#### Program starts here ####
secure = readRDS("secure.rds")
main(5,secure[1],secure[2],secure[3])
