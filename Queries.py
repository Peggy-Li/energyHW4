import os
import psycopg2


conn = psycopg2.connect("dbname=postgres host=/home/" + os.environ['USER'] + "/postgres")
cur = conn.cursor()
"""
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

"""
# c) Calculate the percent of transportation CO2 emissions should be attributed to household vehicles for each month of the survey (3/2008 – 04/2009).
#You should find the number of households represented in each month of dayv2.csv

total_transportation_values_query = """SELECT date, (value*1000000) AS MetricTonsCO2
FROM eia_co2_transportation
WHERE column_order = 12 AND date >= 200803 and date <= 200904"""

#edit: should be count of households that had at least 1 trip in a household vehicle from day file for that particular month

GAS_TO_CO2 = 8.887 * 1000
US_HOUSEHOLDS = 117538000

# get number of households overall
# SELECT COUNT(*) FROM (SELECT "HOUSEID", COUNT(*) FROM daytrip GROUP BY "HOUSEID") X;

# get number of households by month
'SELECT COUNT(*) FROM (SELECT "HOUSEID", SUM("TRPMILES") FROM daytrip WHERE "TDAYDATE"%100 IN (%d) GROUP BY "HOUSEID") X;' % months

"""
month_lengths = [28, 30, 31] # add 29

month28 = '2'
month30 = '4, 6, 9, 11'
month31 = '1, 3, 5, 7, 8, 10, 12'

months = {28: month28, 30: month30, 31: month31}
"""

months = {1:31, 2:28, 3:31, 4:30, 5:31, 6:30, 7:31, 8:31, 9:30, 10:31, 11:30, 12:31}

survey_months = [200803, 200804, 200805, 200806, 200807, 200808, 200809, 200810, 200811, 200812, 200901, 200902, 200903, 200904]

for month in survey_months:
    days = month[(month%100)]

    monthly_households_query = 'SELECT COUNT(*) FROM (SELECT "HOUSEID" FROM daytrip WHERE "TDAYDATE" = %d GROUP BY "HOUSEID") X;' % month
    cur.execute(monthly_households_query)
    monthly_households = cur.fetchone()[]

SELECT SUM("GasGallons"
FROM
(SELECT SUM("TRPMILES"), "EPATMPG", (SUM("TRPMILES") / "EPATMPG") AS "GasGallons"
FROM daytrip, vehicle, household
WHERE "TRPMILES" >= 0 AND daytrip."HOUSEID" = household."HOUSEID" AND "TDAYDATE" = month
GROUP BY "HOUSEID", "VEHID") X



answer = []


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



conn.commit()
cur.close()
conn.close()
