import os
import psycopg2

def main():
    conn = psycopg2.connect("dbname=postgres host=/home/" + os.environ['USER'] + "/postgres")
    cur = conn.cursor()

    cur.execute('DROP TABLE IF EXISTS dayv2pub, hhv2pub, perv2pub, vehv2pub, eia_mkwh_2015, eia_co2_electricity_2015, eia_co2_transportation_2015;')
    createTables(cur)

    NHTS_files = ['DAYV2PUB.CSV', 'HHV2PUB.CSV', 'VEHV2PUB.CSV', 'PERV2PUB.CSV']
    EIA_files = ['EIA_CO2_Transportation_2015.csv', 'EIA_CO2_Electricity_2015.csv', 'EIA_MkWh_2015.csv']
    nullValues = ['XXXXX', 'XXXX', 'XXX', 'XX', 'Not Available']

    # Create indexes for each csv for which columns we are keeping
    NHTS_files_indexes = []

    # dayv2pub
    colNames="HOUSEID,PERSONID,FRSTHM,OUTOFTWN,ONTD_P1,ONTD_P2,ONTD_P3,ONTD_P4,ONTD_P5,ONTD_P6,ONTD_P7,ONTD_P8,ONTD_P9,ONTD_P10,ONTD_P11,ONTD_P12,ONTD_P13,ONTD_P14,ONTD_P15,TDCASEID,HH_HISP,HH_RACE,DRIVER,R_SEX,WORKER,TRIPPURP,AWAYHOME,DROP_PRK,DRVR_FLG,EDUC,ENDTIME,HH_ONTD,HHMEMDRV,HHRESP,INTSTATE,MSASIZE,NONHHCNT,NUMONTRP,PAYTOLL,PRMACT,PROXY,PSGR_FLG,R_AGE,STRTTIME,TRACC1,TRACC2,TRACC3,TRACC4,TRACC5,TRACCTM,TRAVDAY,TREGR1,TREGR2,TREGR3,TREGR4,TREGR5,TREGRTM,TRPACCMP,TRPHHACC,TRPHHVEH,TRPTRANS,TRVL_MIN,TRVLCMIN,TRWAITTM,USEINTST,USEPUBTR,VEHID,WHODROVE,WHYFROM,WHYTO,WHYTRP1S,DWELTIME,WHYTRP90,TDTRPNUM,TDWKND,TRPMILES,WTTRDFIN,VMT_MILE,PUBTRANS,HTPPOPDN,GASPRICE,VEHTYPE,HHC_MSA".split(',')

    allCol = "HOUSEID,PERSONID,FRSTHM,OUTOFTWN,ONTD_P1,ONTD_P2,ONTD_P3,ONTD_P4,ONTD_P5,ONTD_P6,ONTD_P7,ONTD_P8,ONTD_P9,ONTD_P10,ONTD_P11,ONTD_P12,ONTD_P13,ONTD_P14,ONTD_P15,TDCASEID,HH_HISP,HH_RACE,DRIVER,R_SEX,WORKER,DRVRCNT,HHFAMINC,HHSIZE,HHVEHCNT,NUMADLT,FLAG100,LIF_CYC,TRIPPURP,AWAYHOME,CDIVMSAR,CENSUS_D,CENSUS_R,DROP_PRK,DRVR_FLG,EDUC,ENDTIME,HH_ONTD,HHMEMDRV,HHRESP,HHSTATE,HHSTFIPS,INTSTATE,MSACAT,MSASIZE,NONHHCNT,NUMONTRP,PAYTOLL,PRMACT,PROXY,PSGR_FLG,R_AGE,RAIL,STRTTIME,TRACC1,TRACC2,TRACC3,TRACC4,TRACC5,TRACCTM,TRAVDAY,TREGR1,TREGR2,TREGR3,TREGR4,TREGR5,TREGRTM,TRPACCMP,TRPHHACC,TRPHHVEH,TRPTRANS,TRVL_MIN,TRVLCMIN,TRWAITTM,URBAN,URBANSIZE,URBRUR,USEINTST,USEPUBTR,VEHID,WHODROVE,WHYFROM,WHYTO,WHYTRP1S,WRKCOUNT,DWELTIME,WHYTRP90,TDTRPNUM,TDWKND,TDAYDATE,TRPMILES,WTTRDFIN,VMT_MILE,PUBTRANS,HOMEOWN,HOMETYPE,HBHUR,HTRESDN,HTHTNRNT,HTPPOPDN,HTEEMPDN,HBRESDN,HBHTNRNT,HBPPOPDN,GASPRICE,VEHTYPE,HH_CBSA,HHC_MSA".split(',')

    NHTS_files_indexes.append(getIndices(colNames, allCol))

    #household
    allCol = "HOUSEID,VARSTRAT,WTHHFIN,DRVRCNT,CDIVMSAR,CENSUS_D,CENSUS_R,HH_HISP,HH_RACE,HHFAMINC,HHRELATD,HHRESP,HHSIZE,HHSTATE,HHSTFIPS,HHVEHCNT,HOMEOWN,HOMETYPE,MSACAT,MSASIZE,NUMADLT,RAIL,RESP_CNT,SCRESP,TRAVDAY,URBAN,URBANSIZE,URBRUR,WRKCOUNT,TDAYDATE,FLAG100,LIF_CYC,CNTTDHH,HBHUR,HTRESDN,HTHTNRNT,HTPPOPDN,HTEEMPDN,HBRESDN,HBHTNRNT,HBPPOPDN,HH_CBSA,HHC_MSA".split(',')
    NHTS_files_indexes.append(range(len(allCol))) # We use all of the columns

    # veh2pub
    colNames="HOUSEID,WTHHFIN,VEHID,HYBRID,MAKECODE,MODLCODE,MSASIZE,OD_READ,TRAVDAY,VEHCOMM,VEHOWNMO,VEHYEAR,WHOMAIN,VEHAGE,PERSONID,HH_HISP,HH_RACE,ANNMILES,HTPPOPDN,BEST_FLG,BESTMILE,BEST_EDT,BEST_OUT,FUELTYPE,GSYRGAL,GSCOST,GSTOTCST,EPATMPG,EPATMPGF,EIADMPG,VEHTYPE,HHC_MSA".split(',')

    allCol = "HOUSEID,WTHHFIN,VEHID,DRVRCNT,HHFAMINC,HHSIZE,HHVEHCNT,NUMADLT,FLAG100,CDIVMSAR,CENSUS_D,CENSUS_R,HHSTATE,HHSTFIPS,HYBRID,MAKECODE,MODLCODE,MSACAT,MSASIZE,OD_READ,RAIL,TRAVDAY,URBAN,URBANSIZE,URBRUR,VEHCOMM,VEHOWNMO,VEHYEAR,WHOMAIN,WRKCOUNT,TDAYDATE,VEHAGE,PERSONID,HH_HISP,HH_RACE,HOMEOWN,HOMETYPE,LIF_CYC,ANNMILES,HBHUR,HTRESDN,HTHTNRNT,HTPPOPDN,HTEEMPDN,HBRESDN,HBHTNRNT,HBPPOPDN,BEST_FLG,BESTMILE,BEST_EDT,BEST_OUT,FUELTYPE,GSYRGAL,GSCOST,GSTOTCST,EPATMPG,EPATMPGF,EIADMPG,VEHTYPE,HH_CBSA,HHC_MSA"
    NHTS_files_indexes.append(getIndices(colNames, allCol))

    # perv2pub
    colNames = "HOUSEID,PERSONID,VARSTRAT,WTPERFIN,SFWGT,HH_HISP,HH_RACE,CNTTDTR,BORNINUS,CARRODE,CONDNIGH,CONDPUB,CONDRIDE,CONDRIVE,CONDSPEC,CONDTAX,CONDTRAV,DELIVER,DIARY,DISTTOSC,DRIVER,DTACDT,DTCONJ,DTCOST,DTRAGE,DTRAN,DTWALK,EDUC,EVERDROV,FLEXTIME,FMSCSIZE,FRSTHM,FXDWKPL,GCDWORK,GRADE,GT1JBLWK,HHRESP,ISSUE,OCCAT,LSTTRDAY,MCUSED,MEDCOND,MEDCOND6,MOROFTEN,NBIKETRP,NWALKTRP,OUTCNTRY,OUTOFTWN,PAYPROF,PRMACT,PROXY,PTUSED,PURCHASE,R_AGE,R_RELAT,R_SEX,SAMEPLC,SCHCARE,SCHCRIM,SCHDIST,SCHSPD,SCHTRAF,SCHTRN1,SCHTRN2,SCHTYP,SCHWTHR,SELF_EMP,TIMETOSC,TIMETOWK,TOSCSIZE,TRAVDAY,USEINTST,USEPUBTR,WEBUSE,WKFMHMXX,WKFTPT,WKRMHM,WKSTFIPS,WORKER,WRKTIME,WRKTRANS,YEARMILE,YRMLCAP,YRTOUS,DISTTOWK,HTPPOPDN,HHC_MSA".split(',')

    allCol = "HOUSEID,PERSONID,VARSTRAT,WTPERFIN,SFWGT,HH_HISP,HH_RACE,DRVRCNT,HHFAMINC,HHSIZE,HHVEHCNT,NUMADLT,WRKCOUNT,FLAG100,LIF_CYC,CNTTDTR,BORNINUS,CARRODE,CDIVMSAR,CENSUS_D,CENSUS_R,CONDNIGH,CONDPUB,CONDRIDE,CONDRIVE,CONDSPEC,CONDTAX,CONDTRAV,DELIVER,DIARY,DISTTOSC,DRIVER,DTACDT,DTCONJ,DTCOST,DTRAGE,DTRAN,DTWALK,EDUC,EVERDROV,FLEXTIME,FMSCSIZE,FRSTHM,FXDWKPL,GCDWORK,GRADE,GT1JBLWK,HHRESP,HHSTATE,HHSTFIPS,ISSUE,OCCAT,LSTTRDAY,MCUSED,MEDCOND,MEDCOND6,MOROFTEN,MSACAT,MSASIZE,NBIKETRP,NWALKTRP,OUTCNTRY,OUTOFTWN,PAYPROF,PRMACT,PROXY,PTUSED,PURCHASE,R_AGE,R_RELAT,R_SEX,RAIL,SAMEPLC,SCHCARE,SCHCRIM,SCHDIST,SCHSPD,SCHTRAF,SCHTRN1,SCHTRN2,SCHTYP,SCHWTHR,SELF_EMP,TIMETOSC,TIMETOWK,TOSCSIZE,TRAVDAY,URBAN,URBANSIZE,URBRUR,USEINTST,USEPUBTR,WEBUSE,WKFMHMXX,WKFTPT,WKRMHM,WKSTFIPS,WORKER,WRKTIME,WRKTRANS,YEARMILE,YRMLCAP,YRTOUS,DISTTOWK,TDAYDATE,HOMEOWN,HOMETYPE,HBHUR,HTRESDN,HTHTNRNT,HTPPOPDN,HTEEMPDN,HBRESDN,HBHTNRNT,HBPPOPDN,HH_CBSA,HHC_MSA".split(',')
    NHTS_files_indexes.append(getIndices(colNames, allCol))

    EIA_files_indexes = [[x for x in range(7)] for __ in range(7)]

            
    for i, myfile in enumerate(NHTS_files):
       #loadTables(myfile, cur, 'XX')
       loadTables2(myfile, cur, NHTS_files_indexes[i], nullValues)

    for i, myfile in enumerate(EIA_files):
       #loadTables(myfile, cur, 'Not Available')
       loadTables2(myfile, cur, EIA_files_indexes[i], nullValues)


    conn.commit()
    cur.close()
    conn.close()

def getIndices(colNames, allCol):
    for n in colNames:
        for i in range(len(allCol)):
            if n == allCol[i]:
                colIndexes.append(i)
                break
    return colIndexes

def createTables(cur):
    cur.execute('CREATE TABLE EIA_MkWh_2015(MSN CHAR(8), Date CHAR(6), Value DOUBLE PRECISION, Column_Order INT, DESCRIPTION VARCHAR(100), UNIT VARCHAR(100));')

    cur.execute('CREATE TABLE EIA_CO2_Electricity_2015(MSN CHAR(8), Date CHAR(6), Value DOUBLE PRECISION, Column_Order INT, DESCRIPTION VARCHAR(100), UNIT VARCHAR(100));')

    cur.execute('CREATE TABLE EIA_CO2_Transportation_2015(MSN CHAR(8), Date CHAR(6), Value DOUBLE PRECISION, Column_Order INT, DESCRIPTION VARCHAR(100), UNIT VARCHAR(100));')

    cur.execute('CREATE TABLE HHV2PUB("HOUSEID" int, "VARSTRAT" int, "WTHHFIN" double precision, "DRVRCNT" int, "CDIVMSAR" int, "CENSUS_D" int, "CENSUS_R" int, "HH_HISP" int, "HH_RACE" int, "HHFAMINC" int, "HHRELATD" int, "HHRESP" int, "HHSIZE" int, "HHSTATE" varchar(2), "HHSTFIPS" int, "HHVEHCNT" int, "HOMEOWN" int, "HOMETYPE" int, "MSACAT" int, "MSASIZE" int, "NUMADLT" int, "RAIL" int, "RESP_CNT" int, "SCRESP" int, "TRAVDAY" int, "URBAN" int, "URBANSIZE" int, "URBRUR" int, "WRKCOUNT" int, "TDAYDATE" int, "FLAG100" int, "LIF_CYC" int, "CNTTDHH" int, "HBHUR" varchar(2), "HTRESDN" int, "HTHTNRNT" int, "HTPPOPDN" int, "HTEEMPDN" int, "HBRESDN" int, "HBHTNRNT" int, "HBPPOPDN" int, "HH_CBSA" varchar(5), "HHC_MSA" varchar(4));')

    cur.execute('CREATE TABLE DAYV2PUB("HOUSEID" int, "PERSONID" int, "FRSTHM" int, "OUTOFTWN" int, "ONTD_P1" int, "ONTD_P2" int, "ONTD_P3" int, "ONTD_P4" int, "ONTD_P5" int, "ONTD_P6" int, "ONTD_P7" int, "ONTD_P8" int, "ONTD_P9" int, "ONTD_P10" int, "ONTD_P11" int, "ONTD_P12" int, "ONTD_P13" int, "ONTD_P14" int, "ONTD_P15" int, "TDCASEID" bigint, "HH_HISP" int, "HH_RACE" int, "DRIVER" int, "R_SEX" int, "WORKER" int, "DRVRCNT" int, "HHFAMINC" int, "HHSIZE" int, "HHVEHCNT" int, "NUMADLT" int, "FLAG100" int, "LIF_CYC" int, "TRIPPURP" varchar(8), "AWAYHOME" int, "CDIVMSAR" int, "CENSUS_D" int, "CENSUS_R" int, "DROP_PRK" int, "DRVR_FLG" int, "EDUC" int, "ENDTIME" int, "HH_ONTD" int, "HHMEMDRV" int, "HHRESP" int, "HHSTATE" varchar(2), "HHSTFIPS" int, "INTSTATE" int, "MSACAT" int, "MSASIZE" int, "NONHHCNT" int, "NUMONTRP" int, "PAYTOLL" int, "PRMACT" int, "PROXY" int, "PSGR_FLG" int, "R_AGE" int, "RAIL" int, "STRTTIME" int, "TRACC1" int, "TRACC2" int, "TRACC3" int, "TRACC4" int, "TRACC5" int, "TRACCTM" int, "TRAVDAY" int, "TREGR1" int, "TREGR2" int, "TREGR3" int, "TREGR4" int, "TREGR5" int, "TREGRTM" int, "TRPACCMP" int, "TRPHHACC" int, "TRPHHVEH" int, "TRPTRANS" int, "TRVL_MIN" int, "TRVLCMIN" int, "TRWAITTM" int, "URBAN" int, "URBANSIZE" int, "URBRUR" int, "USEINTST" int, "USEPUBTR" int, "VEHID" int, "WHODROVE" int, "WHYFROM" int, "WHYTO" int, "WHYTRP1S" int, "WRKCOUNT" int, "DWELTIME" int, "WHYTRP90" int, "TDTRPNUM" int, "TDWKND" int, "TDAYDATE" int, "TRPMILES" double precision, "WTTRDFIN" double precision, "VMT_MILE" double precision, "PUBTRANS" int, "HOMEOWN" int, "HOMETYPE" int, "HBHUR" varchar(2), "HTRESDN" int, "HTHTNRNT" int, "HTPPOPDN" int, "HTEEMPDN" int, "HBRESDN" int, "HBHTNRNT" int, "HBPPOPDN" int, "GASPRICE" double precision, "VEHTYPE" int, "HH_CBSA" varchar(5), "HHC_MSA" varchar(4));')

    cur.execute('CREATE TABLE PERV2PUB("HOUSEID" int, "PERSONID" int, "VARSTRAT" int, "WTPERFIN" double precision, "SFWGT" double precision, "HH_HISP" int, "HH_RACE" int, "DRVRCNT" int, "HHFAMINC" int, "HHSIZE" int, "HHVEHCNT" int, "NUMADLT" int, "WRKCOUNT" int, "FLAG100" int, "LIF_CYC" int, "CNTTDTR" int, "BORNINUS" int, "CARRODE" int, "CDIVMSAR" int, "CENSUS_D" int, "CENSUS_R" int, "CONDNIGH" int, "CONDPUB" int, "CONDRIDE" int, "CONDRIVE" int, "CONDSPEC" int, "CONDTAX" int, "CONDTRAV" int, "DELIVER" int, "DIARY" int, "DISTTOSC" int, "DRIVER" int, "DTACDT" int, "DTCONJ" int, "DTCOST" int, "DTRAGE" int, "DTRAN" int, "DTWALK" int, "EDUC" int, "EVERDROV" int, "FLEXTIME" int, "FMSCSIZE" int, "FRSTHM" int, "FXDWKPL" int, "GCDWORK" numeric, "GRADE" int, "GT1JBLWK" int, "HHRESP" int, "HHSTATE" varchar(2), "HHSTFIPS" int, "ISSUE" int, "OCCAT" int, "LSTTRDAY" int, "MCUSED" int, "MEDCOND" int, "MEDCOND6" int, "MOROFTEN" int, "MSACAT" int, "MSASIZE" int, "NBIKETRP" int, "NWALKTRP" int, "OUTCNTRY" int, "OUTOFTWN" int, "PAYPROF" int, "PRMACT" int, "PROXY" int, "PTUSED" int, "PURCHASE" int, "R_AGE" int, "R_RELAT" int, "R_SEX" int, "RAIL" int, "SAMEPLC" int, "SCHCARE" int, "SCHCRIM" int, "SCHDIST" int, "SCHSPD" int, "SCHTRAF" int, "SCHTRN1" int, "SCHTRN2" int, "SCHTYP" int, "SCHWTHR" int, "SELF_EMP" int, "TIMETOSC" int, "TIMETOWK" int, "TOSCSIZE" int, "TRAVDAY" int, "URBAN" int, "URBANSIZE" int, "URBRUR" int, "USEINTST" int, "USEPUBTR" int, "WEBUSE" int, "WKFMHMXX" int, "WKFTPT" int, "WKRMHM" int, "WKSTFIPS" int, "WORKER" int, "WRKTIME" varchar(7), "WRKTRANS" int, "YEARMILE" int, "YRMLCAP" int, "YRTOUS" int, "DISTTOWK" double precision, "TDAYDATE" int, "HOMEOWN" int, "HOMETYPE" int, "HBHUR" varchar(2), "HTRESDN" int, "HTHTNRNT" int, "HTPPOPDN" int, "HTEEMPDN" int, "HBRESDN" int, "HBHTNRNT" int, "HBPPOPDN" int, "HH_CBSA" varchar(5), "HHC_MSA" varchar(4));')

    cur.execute('CREATE TABLE VEHV2PUB("HOUSEID" int, "WTHHFIN" double precision, "VEHID" int, "DRVRCNT" int, "HHFAMINC" int, "HHSIZE" int, "HHVEHCNT" int, "NUMADLT" int, "FLAG100" int, "CDIVMSAR" int, "CENSUS_D" int, "CENSUS_R" int, "HHSTATE" varchar(2), "HHSTFIPS" int, "HYBRID" int, "MAKECODE" int, "MODLCODE" int, "MSACAT" int, "MSASIZE" int, "OD_READ" int, "RAIL" int, "TRAVDAY" int, "URBAN" int, "URBANSIZE" int, "URBRUR" int, "VEHCOMM" int, "VEHOWNMO" double precision, "VEHYEAR" int, "WHOMAIN" int, "WRKCOUNT" int, "TDAYDATE" int, "VEHAGE" int, "PERSONID" int, "HH_HISP" int, "HH_RACE" int, "HOMEOWN" int, "HOMETYPE" int, "LIF_CYC" int, "ANNMILES" double precision, "HBHUR" varchar(2), "HTRESDN" int, "HTHTNRNT" int, "HTPPOPDN" int, "HTEEMPDN" int, "HBRESDN" int, "HBHTNRNT" int, "HBPPOPDN" int, "BEST_FLG" int, "BESTMILE" double precision, "BEST_EDT" int, "BEST_OUT" int, "FUELTYPE" int, "GSYRGAL" int, "GSCOST" double precision, "GSTOTCST" int, "EPATMPG" double precision, "EPATMPGF" int, "EIADMPG" double precision, "VEHTYPE" int, "HH_CBSA" varchar(5), "HHC_MSA" varchar(4));')

def loadTables(filename, cur, null_string):
    print "IN loadTables, null_string = %s" %null_string
    chunksize = 1000
    fid = 1
    filesmade = []
    with open('our_subset/%s' %(filename)) as infile:
        f = open('%s%d' %(filename, fid), 'w')
        print f
        for i, line in enumerate(infile):
            if i!=0:
                f.write(line)
                if not i%chunksize:
                    tablename = filename.split('.')[0]
                    f.close()
                    f = open('%s%d' %(filename, fid), 'r')
                    cur.copy_expert("COPY %s FROM STDIN WITH (FORMAT csv, NULL'%s')" %(tablename, null_string), f)
                    os.remove(f.name)
                    f.close()
                    fid += 1
                    f = open('%s%d' %(filename, fid), 'w')
        f.close()
        f = open('%s%d' %(filename, fid), 'r')
        cur.copy_expert("COPY %s FROM STDIN WITH (FORMAT csv, NULL'%s')" %(tablename, null_string), f)
        os.remove(f.name)
        f.close()

def loadTables2(filename, cur, valuesIndex, nullValues):
    with open('our_subset/%s' %(filename)) as f:
        counter = 0
        next(f) # or f.readline()
        insertStatement = "INSERT INTO %s VALUES (" %(filename,)
        valuesList = []
        for line in f:
            counter += 1
            lineValues = line.split(',')
            values = []
            for i in range(len(lineValues)):
                if i in valuesIndex:
                    v = None
                    try:
                        v = int(lineValues[i])
                    except:
                        try:
                            v = float(lineValues[i]);
                        except:
                            pass

                    if v is not None:
                        values.append(linesValues[i])
                    else:
                        if linesValues in nullValues:
                            values.append('NULL')
                        else:
                            values.append("'" + lineValues[i] + "'")

            if counter%1000 == 0:
                # make the insert statement
                insertStatement += values.join(',')
                insertStatement += '), '
                # remove last comma and add semicolon
                insertStatement = insertStatement[0:-1]
                insertStatement += ";"
                cur.execute(insertStatement) # suppose to pass in values in second argument of execute for safety (sql injection) but meh
        if counter%1000 != 0: # If we don't have exact multiple of 1000
            insertStatement += values.join(',')
            insertStatement += '), '
            # remove last comma and add semicolon
            insertStatement = insertStatement[0:-1]
            insertStatement += ";"
            cur.execute(insertStatement) # suppose to pass in values in second argument of execute for safety (sql injection) but meh


main()
