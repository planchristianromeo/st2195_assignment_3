### This is the Relational Database Assignment
#### We will be handling data using SQL, R, Python
#### We will be using the **Data Expo 2009** and **<a href="https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/HG7NV7"**>Harvard Dataverse</a>


## SQL CODE:
# 1

SELECT TailNum, AVG(DepDelay) as Ave_Delay FROM ontime
	WHERE Cancelled = 0
	AND Diverted = 0
GROUP BY tailnum
	HAVING AVG(DepDelay) > 0
ORDER BY Ave_Delay

________________

# 2 
________________
**(No city)
	-- Solution: 
	-- SELECT o.Origin, o.Dest, o.Distance, 
			-- ROUND(Distance Formula using Lat and Long) AS Origin_comp
			-- ROUND(Distance Formula using Lat and Long) AS Dest_comp
		-- 1. SELECT DISTINCT Origin, Dest, Distance FROM ontime o
			--	WHERE Dest = ...
			--	AND Origin = ...
		-- 2. LEFT JOIN airports a1 ON o.Origin = a1.state
		-- 3. LEFT JOIN airports a2 ON o.Dest = a2.state
**
_________________		
SELECT Dest, COUNT(Dest) AS Number_of_In_Flights 
FROM ontime
	WHERE Cancelled = 0
GROUP BY Dest
ORDER BY Number_of_Flights
_________________

# 3
SELECT o.UniqueCarrier, c.Description, sum(Cancelled) AS total_cancelled 
FROM ontime o

LEFT JOIN carriers c ON o.UniqueCarrier = c.Code
GROUP BY o.UniqueCarrier
ORDER BY total_cancelled
_________________
	
# 4
_________________
SELECT a2.UniqueCarrier, c.Description, a2.total_flights, a2.total_cancelled_flights, (CAST(a2.total_cancelled_flights AS FLOAT)/CAST(a2.total_flights AS FLOAT)) AS ratio
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
	
LEFT JOIN carriers c ON a2.UniqueCarrier = c.Code
ORDER BY ratio
________________
#### Shorter Version

SELECT UniqueCarrier, c.Description, count(UniqueCarrier) AS total_flights, sum(cancelled) AS total_cancelled_flights, 
		(CAST(count(UniqueCarrier) as FLOAT)/ CAST(sum(cancelled) AS FLOAT)) AS ratio
FROM ontime o
JOIN carriers c ON o.UniqueCarrier = c.Code
GROUP BY UniqueCarrier
ORDER BY ratio
