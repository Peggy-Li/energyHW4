import os
import psycopg2


month_lengths = [28, 30, 31] # add 29

month28 = '2'
month30 = '4, 6, 9, 11'
month31 = '1, 3, 5, 7, 8, 10, 12'

months_by_num_days = {28: month28, 30: month30, 31: month31}

start_date = 200803
end_date = 200904

GAS_TO_CO2 = 8.887 / 1000
US_HOUSEHOLDS = 117538000

months = {1:31, 2:28, 3:31, 4:30, 5:31, 6:30, 7:31, 8:31, 9:30, 10:31, 11:30, 12:31}

survey_months = [200803, 200804, 200805, 200806, 200807, 200808, 200809, 200810, 200811, 200812, 200901, 200902, 200903, 200904]
survey_months_string = "200803, 200804, 200805, 200806, 200807, 200808, 200809, 200810, 200811, 200812, 200901, 200902, 200903, 200904"

def main():

    conn = psycopg2.connect("dbname=postgres host=/home/" + os.environ['USER'] + "/postgres")
    cur = conn.cursor()

    #queryA(cur)
    #queryB(cur)
    #queryC(cur)
    queryD(cur)

    conn.commit()
    cur.close()
    conn.close()
#a) Calculate the percent of individuals that travel less than 5 - 100 miles a day for every 5 mile increments (e.g. 5, 10, 15, ..., 95, 100).
def queryA(cur):
    answer = []
    for mileage_limit in range(5,101, 5):
        
        mileage_limit_values = []
        total_values = []
        for month_length in month_lengths:    
            month_match_query_part = months_by_num_days[month_length]
        
            total_query = """
            SELECT (%d*COUNT(*))
            FROM
            (SELECT daytrip."HOUSEID", daytrip."PERSONID", "TDAYDATE", SUM("TRPMILES") AS "PersonMilesInDay"
            FROM daytrip, household
            WHERE daytrip."HOUSEID" = household."HOUSEID" AND "TDAYDATE"%%100 IN (%s) AND "TRPMILES" >= 0
            GROUP BY daytrip."HOUSEID", daytrip."PERSONID", "TDAYDATE") PersonMilesPerDay;""" % (month_length, month_match_query_part)
            
            mileage_limit_query = total_query[:-2] + ' WHERE "PersonMilesInDay" < %d;' % mileage_limit

            #print total_query
            #print mileage_limit_query

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

#b) Calculate the average fuel economy of all miles traveled for trips less than specific distances from previous problem. Only consider trips that utilize a household vehicle (VEHID is 1 or larger), use the EPA combined fuel economy (EPATMPG) for the particular vehicle.

def queryB(cur):
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

# c) Calculate the percent of transportation CO2 emissions should be attributed to household vehicles for each month of the survey
def queryC(cur):
    answer = []

    total_transportation_values_query = """SELECT (value*1000000) AS MetricTonsCO2
    FROM eia_co2_transportation
    WHERE column_order = 12 AND date IN (%s)
    ORDER BY date""" % survey_months_string

    cur.execute(total_transportation_values_query)
    total_transportation_monthly_values = cur.fetchall()
    total_transportation_monthly_values = [i[0] for i in total_transportation_monthly_values]

    for i,month in enumerate(survey_months):
        num_days = months[(month%100)]

        monthly_households_query = """
        SELECT COUNT(*) FROM (SELECT daytrip.\"HOUSEID\" FROM daytrip, household WHERE \"TDAYDATE\" = %d AND \"TRPMILES\" >= 0 AND daytrip.\"HOUSEID\" = household.\"HOUSEID\" AND daytrip.\"VEHID\" >= 1 GROUP BY daytrip.\"HOUSEID\") X;""" % month
        cur.execute(monthly_households_query)
        monthly_households = cur.fetchone()[0]

        household_monthly_CO2_query = """
        SELECT ((SUM(\"GasGallons\")) * %d * %f * %d / %d)  AS MetricTonsCO2
        FROM
        (SELECT (SUM(\"TRPMILES\") / \"EPATMPG\") AS \"GasGallons\"
        FROM daytrip, vehicle, household
        WHERE \"TRPMILES\" >= 0 AND \"TDAYDATE\" = %d AND daytrip.\"HOUSEID\" = vehicle.\"HOUSEID\" AND daytrip.\"HOUSEID\" = household.\"HOUSEID\" AND daytrip.\"VEHID\" >= 1 AND daytrip.\"VEHID\" = vehicle.\"VEHID\"
        GROUP BY daytrip.\"HOUSEID\", daytrip.\"VEHID\", \"EPATMPG\" ORDER BY \"GasGallons\") X;""" % (num_days, GAS_TO_CO2, US_HOUSEHOLDS, monthly_households, month)

        cur.execute(household_monthly_CO2_query)
        household_monthly_CO2_value = cur.fetchone()[0]
        
        answer.append(float(household_monthly_CO2_value) / total_transportation_monthly_values[i] * 100)

    print "QUERY C"
    print answer

# From Nitta on Piazza: I am not positive the number you will end up seeing, but about 55% or so of CO2 comes form gasoline. If you account for upstream then you would expect to see something like 80% of that be actually burned at the vehicle. So you get something like 44% burned from all gasoline. Residents are a big portion of that, but I'm not positive how much. Another approach, if you go from the rough estimate of households burning about 84gallons a month, then multiply by the number of households, and the CO2 factor, this might give you a ball park. Compare that number to the table, and you should have an estimate.


def queryD(cur):

# Plug-in hybrid vehicles have recently become commercially available; these vehicles operate in a purely electric mode, or in a hybrid mode. Assume that every vehicle is a plug-in hybrid that has X miles of all electric range. The first X miles in a given day will be driven all electric and the remainder will be at the fuel economy listed for the particular vehicle. The Energy Efficiency Ratio (EER) is used to estimate the amount of electricity an equivalent electric vehicle will consume. Assuming an EER of 3.0 and 33.1kWh per gallon of gasoline, you can calculate the equivalent energy efficiency in miles/kWh by multiplying the EPA combined fuel economy by 0.090634441. Calculate the change of CO2 over the months of the survey if every household vehicle were plug- in hybrids with 20 mile electric range. Calculate for 40 and 60 mile electric ranges as well.




# You have to calculate how much electricity is consumed in kWh by using the description above. Once you have kWh, you can figure out how much CO2 attributed per kWh by looking at the EIA data.

# For 3d you are considering all sources for the electricity. You would use the totals ELETPUS and TXEIEUS.

# you need to determine the CO2 that will not be created for the first 20 miles since you won't be burning gasoline. You also then need to determine how much CO2 is created because of the electricity consumed. You are looking for that delta.

# Not exactly sure, but if you figure out the percentage of trip miles that are in the first 20 miles. That number times 2/3 would be probably a ball park. The EER is 3 so it should consume roughly 1/3 gasoline equivalent for those miles. So a 2/3 reduction for those miles. Does that make sense? 

# You are looking at the reduction in CO2. So CO2 that would have been generated by gas minus the CO2 generated by electricity.
    print "QueryD"

main()
