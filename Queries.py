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

    queryA(cur)
    queryB(cur)
    queryC(cur)
    queryD(cur)

    conn.commit()
    cur.close()
    conn.close()

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
            (SELECT dayv2pub.HOUSEID, dayv2pub.PERSONID, dayv2pub.TDAYDATE, SUM(TRPMILES) AS "PersonMilesInDay"
            FROM dayv2pub, hhv2pub
            WHERE dayv2pub.HOUSEID = hhv2pub.HOUSEID AND dayv2pub.TDAYDATE%%100 IN (%s) AND TRPMILES >= 0
            GROUP BY dayv2pub.HOUSEID, dayv2pub.PERSONID, dayv2pub.TDAYDATE) PersonMilesPerDay;""" % (month_length, month_match_query_part)
            
            mileage_limit_query = total_query[:-1] + ' WHERE "PersonMilesInDay" < %d;' % mileage_limit

            cur.execute(mileage_limit_query)
            mileage_limit_values.append(cur.fetchone()[0])
            
            cur.execute(total_query)
            total_values.append(cur.fetchone()[0])

        percentage = float(sum(mileage_limit_values)) / sum(total_values) * 100.0
        answer.append(percentage)

    print "QUERY A"
    print answer

#b) Calculate the average fuel economy of all miles traveled for trips less than specific distances from previous problem. Only consider trips that utilize a household vehicle (VEHID is 1 or larger), use the EPA combined fuel economy (EPATMPG) for the particular vehicle.

def queryB(cur):
    queryB = """
    SELECT SUM(totalmilespervehicle * EPATMPG) / SUM(totalmilespervehicle)
    FROM (SELECT dayv2pub.HOUSEID, dayv2pub.VEHID, SUM(TRPMILES) AS totalmilespervehicle, EPATMPG
    FROM dayv2pub, vehv2pub
    WHERE dayv2pub.HOUSEID = vehv2pub.HOUSEID AND dayv2pub.VEHID = vehv2pub.VEHID AND TRPMILES >= 0 AND TRPMILES < %d
    GROUP BY dayv2pub.HOUSEID, dayv2pub.VEHID, EPATMPG
    HAVING dayv2pub.VEHID >= 1) X;
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

    total_transportation_values_query = """
    SELECT (value*1000000) AS MetricTonsCO2
    FROM eia_co2_transportation_2015
    WHERE column_order = 12 AND date IN (%s)
    ORDER BY date
    """ % survey_months_string

    cur.execute(total_transportation_values_query)
    total_transportation_monthly_values = cur.fetchall()
    total_transportation_monthly_values = [i[0] for i in total_transportation_monthly_values]

    for i,month in enumerate(survey_months):
        num_days = months[(month%100)]

        monthly_households_query = """
        SELECT COUNT(*) FROM (SELECT dayv2pub.HOUSEID FROM dayv2pub, hhv2pub WHERE dayv2pub.TDAYDATE = %d AND TRPMILES >= 0 AND dayv2pub.HOUSEID = hhv2pub.HOUSEID AND dayv2pub.VEHID >= 1 GROUP BY dayv2pub.HOUSEID) X;
        """ % month
        cur.execute(monthly_households_query)
        monthly_households = cur.fetchone()[0]

        household_monthly_CO2_query = """
        SELECT ((SUM(\"GasGallons\")) * %d * %f * %d / %d)  AS MetricTonsCO2
        FROM
        (SELECT (SUM(TRPMILES) / EPATMPG) AS \"GasGallons\"
        FROM dayv2pub, vehv2pub, hhv2pub
        WHERE TRPMILES >= 0 AND dayv2pub.TDAYDATE = %d AND dayv2pub.HOUSEID = vehv2pub.HOUSEID AND dayv2pub.HOUSEID = hhv2pub.HOUSEID AND dayv2pub.VEHID >= 1 AND dayv2pub.VEHID = vehv2pub.VEHID AND DRVR_FLG = 1
        GROUP BY dayv2pub.HOUSEID, dayv2pub.VEHID, EPATMPG) X;
        """ % (num_days, GAS_TO_CO2, US_HOUSEHOLDS, monthly_households, month)

        cur.execute(household_monthly_CO2_query)
        household_monthly_CO2_value = cur.fetchone()[0]
        
        answer.append(float(household_monthly_CO2_value) / total_transportation_monthly_values[i] * 100)

    print "QUERY C"
    print answer


def queryD(cur):
    all_answers = []
    for limit in (20, 40, 60):
        answer = []
        for i,month in enumerate(survey_months):
            num_days = months[(month%100)]
            query = """
            SELECT sum(sumtrpmiles / epatmpg), sum(sumtrpmiles / (epatmpg * 0.090634441))
            FROM (SELECT d.houseid, d.vehid, sum(d.trpmiles) as sumtrpmiles, v.epatmpg
                FROM dayv2pub as d, vehv2pub as v
                WHERE d.houseid=v.houseid and d.vehid=v.vehid and d.vehid >= 1 and d.drvr_flg = 01 and d.tdaydate = %d 
                GROUP BY d.vehid, d.houseid, v.epatmpg
                HAVING sum(d.trpmiles) <= %d) as sumtrp
            ;
            """ % (month, limit)
            cur.execute(query)
            mytuple = cur.fetchone()
            sum_gallons_used_for_households_in_survey_for_month_miles_lte_20 = mytuple[0]
            sum_kWh_used_for_households_in_survey_for_month_miles_lte_20 =  mytuple[1]

            query = """
            SELECT sum(20 / epatmpg), sum(20 / (epatmpg * 0.090634441))
            FROM (SELECT d.houseid, d.vehid, sum(d.trpmiles) as sumtrpmiles, v.epatmpg
                FROM dayv2pub as d, vehv2pub as v
                WHERE d.houseid=v.houseid and d.vehid=v.vehid and d.vehid >= 1 and d.drvr_flg = 01 and d.tdaydate = 200804 
                GROUP BY d.vehid, d.houseid, v.epatmpg
                HAVING sum(d.trpmiles) > 20) as sumtrp
            ;
            """ 
            cur.execute(query)
            mytuple = cur.fetchone()
            sum_gallons_used_for_households_in_survey_for_month_miles_gt_20 = mytuple[0]
            sum_kWh_used_for_households_in_survey_for_month_miles_gt_20 =  mytuple[1]
            
            query = """
            SELECT value
            FROM eia_co2_electricity_2015
            WHERE date = %d AND column_order = 9;
            """ % month
            cur.execute(query)
            total_co2_produced_for_month = cur.fetchone()[0]

            query = """
            SELECT value
            FROM eia_mkwh_2015
            WHERE date = %d AND column_order = 13;
            """ % month
            cur.execute(query)
            total_electricity_produced_for_month = cur.fetchone()[0]

            monthly_households_query = """
            SELECT COUNT(*)
            FROM (SELECT d.houseid
                FROM dayv2pub as d, hhv2pub as h
                WHERE d.tdaydate = %d AND d.trpmiles >= 0 AND d.houseid = h.houseid AND d.vehid >= 1 
                GROUP BY d.houseid) tempname;
            """ % month
            cur.execute(monthly_households_query)
            monthly_households = cur.fetchone()[0]

            co2_from_electricity_used_by_car_in_survey_for_month = (sum_kWh_used_for_households_in_survey_for_month_miles_lte_20 + sum_kWh_used_for_households_in_survey_for_month_miles_gt_20) * total_co2_produced_for_month / total_electricity_produced_for_month

            co2_not_produced_by_car_in_survey_for_month = (sum_gallons_used_for_households_in_survey_for_month_miles_lte_20 + sum_gallons_used_for_households_in_survey_for_month_miles_gt_20) * GAS_TO_CO2
            change = (co2_from_electricity_used_by_car_in_survey_for_month - co2_not_produced_by_car_in_survey_for_month) * num_days * US_HOUSEHOLDS / monthly_households

            answer.append(change)


        print "QUERYD"        
        all_answers.append(answer)
    print all_answers # prints an array of 3 arrays for part D


main()
