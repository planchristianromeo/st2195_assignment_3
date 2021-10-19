library(DBI)
library(dplyr)

setwd("C:/Users/chris/repositories/st2195_assignment_3/r_sql")


plane <- dbConnect(RSQLite::SQLite(), "r_airline.db")


ontime_2000 = read.csv('../Datasets/2000.csv.bz2')
ontime_2001 = read.csv('../Datasets/2001.csv.bz2')
ontime_2002 = read.csv('../Datasets/2002.csv.bz2')
ontime_2003 = read.csv('../Datasets/2003.csv.bz2')
ontime_2004 = read.csv('../Datasets/2004.csv.bz2')
ontime_2005 = read.csv('../Datasets/2005.csv.bz2')

airport_data = read.csv('../Datasets/airports.csv')
plane_data = read.csv('../Datasets/plane-data.csv')
carrier_data = read.csv('../Datasets/carriers.csv')

# Combine all the tables
ontime = rbind(ontime_2000, ontime_2001, ontime_2002, ontime_2003,
                        ontime_2004, ontime_2005)
#View(ontime)
#unique(ontime[c('Year')])


# copy the dataframes into tables in the database
dbWriteTable(plane, "airport_data", airport_data)
dbWriteTable(plane, "plane_data", plane_data)
dbWriteTable(plane, "carrier_data", carrier_data)
dbWriteTable(plane, "ontime", ontime)

dbListTables(plane)
dbReadTable(plane, "airport_data")
dbListFields(plane, "airport_data")

# 1 - Which plane model has the lowest associated 
#     average departure delay (excluding cancelled and diverted flights)?

dbGetQuery(plane, "SELECT DISTINCT TailNum FROM ontime")

q1 <- dbGetQuery(plane, 
                 "SELECT TailNum, AVG(DepDelay) as Ave_Delay FROM ontime 
                    WHERE Cancelled = 0 
                    AND Diverted = 0 
                  GROUP BY tailnum 
                  HAVING AVG(DepDelay) > 0 
                  ORDER BY Ave_Delay
                  LIMIT 10")
write.csv(as.data.frame(q1),"../r_sql/q1_r.csv", row.names = FALSE)

# 2 - Which city has the highest number of inbound flights (excluding 
#     cancelled flights)?
q2 <- dbSendQuery(plane, 
                "SELECT Dest, COUNT(Dest) AS number_of_In_Flights FROM ontime 
                    WHERE Cancelled = 0 
                 GROUP BY Dest 
                 ORDER BY number_of_In_Flights")
q2.1 = dbFetch(q2)

write.csv(as.data.frame(q2.1),"../r_sql/q2_r.csv", row.names = FALSE)


## 3 - Which carrier has the highest number of cancelled flights?


q3 <- dbGetQuery(plane, "SELECT o.UniqueCarrier, c.Description, sum(Cancelled) AS total_cancelled  
                        FROM ontime o 
                        LEFT JOIN carrier_data c ON o.UniqueCarrier = c.Code 
                        GROUP BY o.UniqueCarrier 
                        ORDER BY total_cancelled")
write.csv(as.data.frame(q3),"../r_sql/q3_r.csv", row.names = FALSE)

# 4 - Which carrier has the highest number of cancelled flights, relative to 
    # their number of total flights?

q4.1 <- dbGetQuery(plane, 
                   "SELECT a2.UniqueCarrier, c.Description, a2.total_flights, a2.total_cancelled_flights, (CAST(a2.total_cancelled_flights AS FLOAT)/CAST(a2.total_flights AS FLOAT)) AS ratio
                  	FROM
                  	(SELECT o.UniqueCarrier, count(o.UniqueCarrier) AS total_flights, total_cancelled_flights
                  		FROM ontime o
                    	JOIN 
                    	 (SELECT UniqueCarrier, count(UniqueCarrier) AS total_cancelled_flights
                    		FROM ontime
                    		WHERE Cancelled = 1
                    		GROUP BY UniqueCarrier) AS a1
                    	ON o.UniqueCarrier = a1.UniqueCarrier
                  	GROUP BY o.UniqueCarrier) AS a2
                    LEFT JOIN carrier_data c ON a2.UniqueCarrier = c.Code
                    ORDER BY ratio")

q4.2 <- dbGetQuery(plane, "SELECT UniqueCarrier, c.Description, count(UniqueCarrier) AS total_flights, sum(cancelled) AS total_cancelled_flights, 
                          (CAST(sum(cancelled) AS FLOAT)/CAST(count(UniqueCarrier) as FLOAT)) AS ratio
                          FROM ontime o
                          JOIN carrier_data c ON o.UniqueCarrier = c.Code
                          GROUP BY UniqueCarrier
                          ORDER BY ratio")
write.csv(as.data.frame(q4.2),"../r_sql/q4_r.csv", row.names = FALSE)

#---------------------------dplyr()----------------
# Increase the memory allocated to R

if(.Platform$OS.type == "windows") withAutoprint({
  memory.size()
  memory.size(TRUE)
  memory.limit()
})
memory.limit(size=56000)
# ________________________________________________

airport_db <- as.data.frame(tbl(plane,"airport_data"))
plane_db <- as.data.frame(tbl(plane, "plane_data"))
carrier_db <- as.data.frame(tbl(plane, "carrier_data"))
ontime_db <- as.data.frame(tbl(plane, "ontime"))

# Q1
q1.dplyr <- ontime_db %>% 
  filter(Cancelled == 0, Diverted == 0,!is.na(TailNum)) %>%
  group_by(TailNum)%>%
  summarise(avg_delay = mean(DepDelay, na.rm = TRUE)) %>%
  select(TailNum, avg_delay)%>%
  arrange((avg_delay)) %>%
  filter(avg_delay > 0)
  
q1.dplyr


# Q2
q2.dplyr <- ontime_db %>%
  filter(Cancelled == 0) %>%
  group_by(Dest) %>%
  count(Dest, sort = TRUE)%>%
  top_n(n,10)

q2.dplyr

# Q3
q3.dplyr <- left_join(ontime_db, carrier_db, by = c("UniqueCarrier" = "Code")) %>%
  group_by(Description) %>%
  summarise(number_cancelled_flights = sum(Cancelled, na.rm = TRUE)) %>%
  arrange(desc(number_cancelled_flights))

q3.dplyr


View(carrier_db)

# Q4
# View it as how you would subset the data (from which data of which data of which data..)

q4.dplyr <- left_join(ontime_db, carrier_db, by = c("UniqueCarrier" = "Code")) %>%
  group_by(Description) %>%
  summarise(number_cancelled_flights = sum(Cancelled, na.rm = TRUE),
            number_flights=n(),
            ratio = sum(Cancelled, na.rm = TRUE)/n())
q4.dplyr