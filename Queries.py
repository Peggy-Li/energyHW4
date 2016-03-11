import os
import psycopg2


conn = psycopg2.connect("dbname=postgres host=/home/" + os.environ['USER'] + "/postgres")
cur = conn.cursor()

#a) Calculate the percent of individuals that travel less than 5 - 100 miles a day for every 5 mile increments (e.g. 5, 10, 15, ..., 95, 100).


# need to fix this query to get all of the people...it's not the count of people, but of "data days"

cur.execute("""
SELECT COUNT(*) FROM person;
""")
peopleTotal = cur.fetchone()

SELECT
FROM
WHERE 

month28 = ['02']
month30 = ['04', '06', '09', '11']
month31 = ['01', '03', '05', '07', '08', '10', '12']

CAST("TDAYDATE" AS VARCHAR(6)) LIKE '%01' OR CAST("TDAYDATE" AS VARCHAR(6)) LIKE '%03'

SELECT (28*COUNT(*)) AS Count28
FROM
(SELECT daytrip."HOUSEID", daytrip."PERSONID", "TDAYDATE", SUM("TRPMILES") AS "PersonMilesInDay"
FROM daytrip, household
WHERE daytrip."HOUSEID" = household."HOUSEID" AND CAST("TDAYDATE" AS VARCHAR(6)) LIKE '%02'
GROUP BY daytrip."HOUSEID", daytrip."PERSONID", "TDAYDATE") PersonMilesPerDay
WHERE "PersonMilesInDay" < 5;




SELECT "TDAYDATE", "HOUSEID" FROM household WHERE CAST("TDAYDATE" AS VARCHAR(6)) LIKE '%02'






# LIKE, SIMILAR TO, ANY


#You want to look at it on a day by day basis. Consider individuals driving on another day as a separate individual. 
#You may want to do multiple queries (one for specific months) to do 31 day months, one for 30 days etc.


answer = []
for upper_limit in range(5, 101, 5):
    cur.execute(queryA % upper_limit)
    answer.append(cur.fetchone()) 

print answer

#b) Calculate the average fuel economy of all miles traveled for trips less than specific distances from previous problem. Only consider trips that utilize a household vehicle (VEHID is 1 or larger), use the EPA combined fuel economy (EPATMPG) for the particular vehicle.

queryB = """
SELECT SUM(totalmilespervehicle * "EPATMPG") / SUM(totalmilespervehicle)
FROM (SELECT daytrip."HOUSEID", daytrip."VEHID", SUM("TRPMILES") AS totalmilespervehicle, "EPATMPG"
FROM daytrip, vehicle
WHERE daytrip."HOUSEID" = vehicle."HOUSEID" AND daytrip."VEHID" = vehicle."VEHID" AND "TRPMILES" >= 0 AND "TRPMILES" < %d
GROUP BY daytrip."HOUSEID", daytrip."VEHID", "EPATMPG"
HAVING daytrip."VEHID" >= 1) X;
"""

answer = []
for upper_limit in range(5, 101, 5):
    cur.execute(queryB % upper_limit)
    answer.append(cur.fetchone()) 

print answer





conn.commit()
cur.close()
conn.close()
