import os
import psycopg2


conn = psycopg2.connect("dbname=postgres host=/home/" + os.environ['USER'] + "/postgres")
cur = conn.cursor()

#a) Calculate the percent of individuals that travel less than 5 - 100 miles a day for every 5 mile increments (e.g. 5, 10, 15, ..., 95, 100).

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
