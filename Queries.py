import os
import psycopg2


conn = psycopg2.connect("dbname=postgres host=/home/" + os.environ['USER'] + "/postgres")
cur = conn.cursor()

#a) Calculate the percent of individuals that travel less than 5 - 100 miles a day for every 5 mile increments (e.g. 5, 10, 15, ..., 95, 100).

month_lengths = [28, 30, 31] # add 29

month28 = '2'
month30 = '4, 6, 9, 11'
month31 = '1, 3, 5, 7, 8, 10, 12'

months = {28: month28, 30: month30, 31: month31}

answer = []
for mileage_limit in range(5,101, 5):
    
    mileage_limit_values = []
    total_values = []
    for month_length in month_lengths:    
        month_match_query_part = months[month_length]
    
        total_query = """
        SELECT (%d*COUNT(*))
        FROM
        (SELECT daytrip."HOUSEID", daytrip."PERSONID", "TDAYDATE", SUM("TRPMILES") AS "PersonMilesInDay"
        FROM daytrip, household
        WHERE daytrip."HOUSEID" = household."HOUSEID" AND "TDAYDATE"%%100 IN (%s) AND "TRPMILES" >= 0
        GROUP BY daytrip."HOUSEID", daytrip."PERSONID", "TDAYDATE") PersonMilesPerDay;""" % (month_length, month_match_query_part)
        
        mileage_limit_query = total_query[:-2] + ' WHERE "PersonMilesInDay" < %d;' % mileage_limit

        print total_query
        print mileage_limit_query

        cur.execute(mileage_limit_query)
        mileage_limit_values.append(cur.fetchone()[0])
        
        cur.execute(total_query)
        total_values.append(cur.fetchone()[0])

    percentage = float(sum(mileage_limit_values)) / sum(total_values) * 100.0
    answer.append(percentage)

print "QUERY A"
print answer

#You want to look at it on a day by day basis. Consider individuals driving on another day as a separate individual. 
# check double counting
# who is supposed to be included? all people, respondent, driver (WHODROVE), all passengers

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
    answer.append(cur.fetchone()[0]) 

print "QUERYB"
print answer





conn.commit()
cur.close()
conn.close()
