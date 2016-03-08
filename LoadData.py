import os
import psycopg2

def splitFile(filename):
    chunksize = 1000
    fid = 1
    filesmade = []
    with open(filename) as infile:
        f = open('%s%d' %(filename, fid), 'w')
        for i, line in enumerate(infile):
            f.write(line)
            if not i%chunksize:
                f.close()
                fid += 1
                f = open('%s%d' %(filename, fid), 'w')
        f.close()
        os.remove(f.name)

def main():
    conn = psycopg2.connect("dbname=postgres host=/home/" + os.environ['USER'] + "/postgres")
    cur = conn.cursor()

    createTables(cur)

    files = ['DAYV2PUB.CSV', 'EIA_CO2_Transportation_2015.csv', 'HHV2PUB.CSV', 'VEHV2PUB.CSV', 'EIA_CO2_Electricity_2015.csv', 'EIA_MkWh_2015.csv', 'PERV2PUB.CSV']
    #for i, myfile in enumerate(files):
        #loadTables('/subset/%s' %(myfile, cur)

    conn.commit()
    cur.close()
    conn.close()

def createTables(cur):
    cur.execute('CREATE TABLE EIA_MkWh_2015(MSN CHAR(8), Date CHAR(6), Value DECIMAL(10,3), Column_Order INT, DESCRIPTION VARCHAR(100), UNIT VARCHAR(100));')


#FROM '/home/kburckin/ECS165A/hw4/EIA_MkWh_2015.csv'
#WITH (FORMAT csv, NULL 'Not Available', HEADER TRUE);")

    cur.execute('CREATE TABLE EIA_CO2_Electricity_2015(MSN CHAR(8), Date CHAR(6), Value DECIMAL(10,3), Column_Order INT, DESCRIPTION VARCHAR(100), UNIT VARCHAR(100));')

#cur.execute("COPY ElectricityCO2
#FROM '/home/kburckin/ECS165A/hw4/EIA_CO2_Electricity_2015.csv'
#WITH (FORMAT csv, NULL 'Not Available', HEADER TRUE);")

    cur.execute('CREATE TABLE EIA_CO2_Transportation_2015(Date CHAR(6), Value DECIMAL(10,3), Column_Order INT, DESCRIPTION VARCHAR(100), UNIT VARCHAR(100));')

#cur.execute("COPY TransportationCO2
#FROM '/home/kburckin/ECS165A/hw4/EIA_CO2_Transportation_2015.csv'
#WITH (FORMAT csv, NULL 'Not Available', HEADER TRUE);")


    cur.execute('CREATE TABLE HHV2PUB("HOUSEID" int, "VARSTRAT" int, "WTHHFIN" int, "DRVRCNT" int, "CDIVMSAR" int, "CENSUS_D" int, "CENSUS_R" int, "HH_HISP" int, "HH_RACE" int, "HHFAMINC" int, "HHRELATD" int, "HHRESP" int, "HHSIZE" int, "HHSTATE" varchar(2), "HHSTFIPS" int, "HHVEHCNT" int, "HOMEOWN" int, "HOMETYPE" int, "MSACAT" int, "MSASIZE" int, "NUMADLT" int, "RAIL" int, "RESP_CNT" int, "SCRESP" int, "TRAVDAY" int, "URBAN" int, "URBANSIZE" int, "URBRUR" int, "WRKCOUNT" int, "TDAYDATE" int, "FLAG100" int, "LIF_CYC" int, "CNTTDHH" int, "HBHUR" varchar(2), "HTRESDN" int, "HTHTNRNT" int, "HTPPOPDN" int, "HTEEMPDN" int, "HBRESDN" int, "HBHTNRNT" int, "HBPPOPDN" int, "HH_CBSA" varchar(5), "HHC_MSA" varchar(4));')


    cur.execute('CREATE TABLE DAYV2PUB("HOUSEID" int, "PERSONID" int, "FRSTHM" int, "OUTOFTWN" int, "ONTD_P1" int, "ONTD_P2" int, "ONTD_P3" int, "ONTD_P4" int, "ONTD_P5" int, "ONTD_P6" int, "ONTD_P7" int, "ONTD_P8" int, "ONTD_P9" int, "ONTD_P10" int, "ONTD_P11" int, "ONTD_P12" int, "ONTD_P13" int, "ONTD_P14" int, "ONTD_P15" int, "TDCASEID" bigint, "HH_HISP" int, "HH_RACE" int, "DRIVER" int, "R_SEX" int, "WORKER" int, "DRVRCNT" int, "HHFAMINC" int, "HHSIZE" int, "HHVEHCNT" int, "NUMADLT" int, "FLAG100" int, "LIF_CYC" int, "TRIPPURP" varchar(8), "AWAYHOME" int, "CDIVMSAR" int, "CENSUS_D" int, "CENSUS_R" int, "DROP_PRK" int, "DRVR_FLG" int, "EDUC" int, "ENDTIME" int, "HH_ONTD" int, "HHMEMDRV" int, "HHRESP" int, "HHSTATE" varchar(2), "HHSTFIPS" int, "INTSTATE" int, "MSACAT" int, "MSASIZE" int, "NONHHCNT" int, "NUMONTRP" int, "PAYTOLL" int, "PRMACT" int, "PROXY" int, "PSGR_FLG" int, "R_AGE" int, "RAIL" int, "STRTTIME" int, "TRACC1" int, "TRACC2" int, "TRACC3" int, "TRACC4" int, "TRACC5" int, "TRACCTM" int, "TRAVDAY" int, "TREGR1" int, "TREGR2" int, "TREGR3" int, "TREGR4" int, "TREGR5" int, "TREGRTM" int, "TRPACCMP" int, "TRPHHACC" int, "TRPHHVEH" int, "TRPTRANS" int, "TRVL_MIN" int, "TRVLCMIN" int, "TRWAITTM" int, "URBAN" int, "URBANSIZE" int, "URBRUR" int, "USEINTST" int, "USEPUBTR" int, "VEHID" int, "WHODROVE" int, "WHYFROM" int, "WHYTO" int, "WHYTRP1S" int, "WRKCOUNT" int, "DWELTIME" int, "WHYTRP90" int, "TDTRPNUM" int, "TDWKND" int, "TDAYDATE" int, "TRPMILES" double precision, "WTTRDFIN" double precision, "VMT_MILE" double precision, "PUBTRANS" int, "HOMEOWN" int, "HOMETYPE" int, "HBHUR" varchar(2), "HTRESDN" int, "HTHTNRNT" int, "HTPPOPDN" int, "HTEEMPDN" int, "HBRESDN" int, "HBHTNRNT" int, "HBPPOPDN" int, "GASPRICE" double precision, "VEHTYPE" int, "HH_CBSA" varchar(5), "HHC_MSA" varchar(4));')

    cur.execute('CREATE TABLE PERV2PUB("HOUSEID" int, "PERSONID" int, "VARSTRAT" int, "WTPERFIN" int, "SFWGT" int, "HH_HISP" int, "HH_RACE" int, "DRVRCNT" int, "HHFAMINC" int, "HHSIZE" int, "HHVEHCNT" int, "NUMADLT" int, "WRKCOUNT" int, "FLAG100" int, "LIF_CYC" int, "CNTTDTR" int, "BORNINUS" int, "CARRODE" int, "CDIVMSAR" int, "CENSUS_D" int, "CENSUS_R" int, "CONDNIGH" int, "CONDPUB" int, "CONDRIDE" int, "CONDRIVE" int, "CONDSPEC" int, "CONDTAX" int, "CONDTRAV" int, "DELIVER" int, "DIARY" int, "DISTTOSC" int, "DRIVER" int, "DTACDT" int, "DTCONJ" int, "DTCOST" int, "DTRAGE" int, "DTRAN" int, "DTWALK" int, "EDUC" int, "EVERDROV" int, "FLEXTIME" int, "FMSCSIZE" int, "FRSTHM" int, "FXDWKPL" int, "GCDWORK" numeric, "GRADE" int, "GT1JBLWK" int, "HHRESP" int, "HHSTATE" varchar(2), "HHSTFIPS" int, "ISSUE" int, "OCCAT" int, "LSTTRDAY" int, "MCUSED" int, "MEDCOND" int, "MEDCOND6" int, "MOROFTEN" int, "MSACAT" int, "MSASIZE" int, "NBIKETRP" int, "NWALKTRP" int, "OUTCNTRY" int, "OUTOFTWN" int, "PAYPROF" int, "PRMACT" int, "PROXY" int, "PTUSED" int, "PURCHASE" int, "R_AGE" int, "R_RELAT" int, "R_SEX" int, "RAIL" int, "SAMEPLC" int, "SCHCARE" int, "SCHCRIM" int, "SCHDIST" int, "SCHSPD" int, "SCHTRAF" int, "SCHTRN1" int, "SCHTRN2" int, "SCHTYP" int, "SCHWTHR" int, "SELF_EMP" int, "TIMETOSC" int, "TIMETOWK" int, "TOSCSIZE" int, "TRAVDAY" int, "URBAN" int, "URBANSIZE" int, "URBRUR" int, "USEINTST" int, "USEPUBTR" int, "WEBUSE" int, "WKFMHMXX" int, "WKFTPT" int, "WKRMHM" int, "WKSTFIPS" int, "WORKER" int, "WRKTIME" varchar(7), "WRKTRANS" int, "YEARMILE" int, "YRMLCAP" int, "YRTOUS" int, "DISTTOWK" int, "TDAYDATE" int, "HOMEOWN" int, "HOMETYPE" int, "HBHUR" varchar(2), "HTRESDN" int, "HTHTNRNT" int, "HTPPOPDN" int, "HTEEMPDN" int, "HBRESDN" int, "HBHTNRNT" int, "HBPPOPDN" int, "HH_CBSA" varchar(5), "HHC_MSA" varchar(4));')

    cur.execute('CREATE TABLE VEHV2PUB("HOUSEID" int, "WTHHFIN" int, "VEHID" int, "DRVRCNT" int, "HHFAMINC" int, "HHSIZE" int, "HHVEHCNT" int, "NUMADLT" int, "FLAG100" int, "CDIVMSAR" int, "CENSUS_D" int, "CENSUS_R" int, "HHSTATE" varchar(2), "HHSTFIPS" int, "HYBRID" int, "MAKECODE" int, "MODLCODE" int, "MSACAT" int, "MSASIZE" int, "OD_READ" int, "RAIL" int, "TRAVDAY" int, "URBAN" int, "URBANSIZE" int, "URBRUR" int, "VEHCOMM" int, "VEHOWNMO" int, "VEHYEAR" int, "WHOMAIN" int, "WRKCOUNT" int, "TDAYDATE" int, "VEHAGE" int, "PERSONID" int, "HH_HISP" int, "HH_RACE" int, "HOMEOWN" int, "HOMETYPE" int, "LIF_CYC" int, "ANNMILES" int, "HBHUR" varchar(2), "HTRESDN" int, "HTHTNRNT" int, "HTPPOPDN" int, "HTEEMPDN" int, "HBRESDN" int, "HBHTNRNT" int, "HBPPOPDN" int, "BEST_FLG" int, "BESTMILE" int, "BEST_EDT" int, "BEST_OUT" int, "FUELTYPE" int, "GSYRGAL" int, "GSCOST" int, "GSTOTCST" int, "EPATMPG" int, "EPATMPGF" int, "EIADMPG" int, "VEHTYPE" int, "HH_CBSA" varchar(5), "HHC_MSA" varchar(4));')


#copy_from(file, table, sep='\t', null='\\N', size=8192, columns=None):

def loadTables(filename, cur):
    
    chunksize = 1000
    fid = 1
    filesmade = []
    with open(filename) as infile:
        f = open('%s%d' %(filename, fid), 'w')
        print f
        for i, line in enumerate(infile):
            if i!=0:
                f.write(line)
                if not i%chunksize:
                    tablename = filename.split('.')[0]
                    f.close()
                    f = open('%s%d' %(filename, fid), 'r')
                    cur.copy_expert("COPY %s FROM STDIN WITH (FORMAT csv, NULL 'Not Available')" %tablename, f)
                    os.remove(f.name)
                    f.close()
                    fid += 1
                    f = open('%s%d' %(filename, fid), 'w')
        f.close()

main()
