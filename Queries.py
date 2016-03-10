import psycopg2


conn = psycopg2.connect("dbname=postgres host=/home/" + os.environ['USER'] + "/postgres")
cur = conn.cursor()

#Calculate the percent of individuals that travel less than 5 - 100 miles a day for every 5 mile increments (e.g. 5, 10, 15, ..., 95, 100).




query1 = 

#for i in (5,10,15,...)
    cur.execute(query1);
