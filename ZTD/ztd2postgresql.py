#!/usr/bin/env python
#Gter copyleft 
#Roberto Marzocchi

import sys
import os
import math
import time
import atexit
import shutil,re,glob

from time import strftime , struct_time

from datetime import datetime, timedelta
#nine_hours_from_now = datetime.now() + timedelta(hours=9)

import getopt



space = re.compile(r'\s+')
multiSpace = re.compile(r"\s\s+") 




def convert_timedelta(duration):
    days, seconds = duration.days, duration.seconds
    hours = days * 24 + seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = (seconds % 60)
    return hours, minutes, seconds
    
    

def main():
    nomefile1 = ''
    check_nulli='TRUE'
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hi:n:", ["help", "ifile=" "null="])
    except getopt.GetoptError:
        print 'ztd2postgresql.py -i <nomefile1> -n <TRUE/FALSE> (default TRUE)'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'ztd2postgresql.py -i <nomefile1> -n <TRUE/FALSE> (default TRUE)'
            sys.exit()
        elif opt in ("-i", "--ifile"):
            nomefile1 = arg
        elif opt in ("-n", "--null_man"):
            check_nulli = arg
    print 'Input file is ', nomefile1
    print 'Null data are inserted in the geoDB ', check_nulli
    #print 'Output file is "', outputfile    
    if nomefile1=='':
        print 'ERROR: specify an input file'
        sys.exit()
    #quit()
    #nomefile1 = "ACOR.2000.net1.az"
    
    ###################################
    # expected interval
    intervallo=2
    ###################################
    array=nomefile1.split("/")
    nomesolofile=array[len(array)-1]
    print "The name of the file is", nomesolofile
    id_stazione=nomesolofile.split(".")[0]
    print "The name of the station is", id_stazione
    
    #quit()
    data_ora=[]
    ZTD=[]
    SQM_ZTD=[]
    
    pippo=0





    print "Reading the file",nomefile1
    #leggo le tre colonne dove nella prima c'e' il 2
    i=0
    n=0
    riga_prima=" "
    line= " "




    for riga in file(nomefile1): 
        #print riga
        riga_prima = line
        line = riga
        #print i
        a = line.split()
        #stringa_data="%s%s%s%s%s" %(a[0],a[1],a[2],a[3],a[4])
        #data_temp=struct_time(tm_year=a[0], tm_mon=a[1], tm_mday=a[2], tm_hour=a[3], tm_min=a[4])
        #data_temp=(int(a[0]), int(a[1]), int(a[2]), int(a[3]), int(a[4]))
        data_temp=datetime(int(a[0]), int(a[1]), int(a[2]), int(a[3]), int(a[4]))
        data_ora.append(data_temp.strftime("%Y/%m/%d %H:%M"))
        #print data_ora
        SQM_ZTD.append(float(a[5]))
        ZTD.append(float(a[6]))
        i+=1


    import psycopg2

    # Connect to an existing database
    sys.path.append(os.path.abspath("../"))
    from credenziali import *
    conn = psycopg2.connect(host=ip, dbname=db, user=user, password=pwd, port=port)
    #autocommit
    conn.set_session(autocommit=True)
    cur = conn.cursor()




    if check_nulli=='TRUE':
        data_ora_ok=[]
        ZTD_ok=[]
        SQM_ZTD_ok=[]
        i=0
        j=1
        while i <len(ZTD):
            if i==0:
                data_ora_ok.append(data_ora[i])
                ZTD_ok.append(ZTD[i])
                SQM_ZTD_ok.append(SQM_ZTD[i])
            else :
                data_letta=datetime.strptime(data_ora[i], "%Y/%m/%d %H:%M")
                #print i,j
                #print "i letta",data_letta
                #print "j letta", datetime.strptime(data_ora_ok[j-1], "%Y/%m/%d %H:%M")
                #print data_ora_ok
                data_attesa=datetime.strptime(data_ora_ok[j-1], "%Y/%m/%d %H:%M")+ timedelta(hours=intervallo)
                #print "attesa",data_attesa
                #quit()
                delta=data_letta - data_attesa
                #print delta
                ########################################
                #normally we read a ZTD every two hours
                ########################################
                if data_letta==data_attesa:
                    ora_letta=datetime.strptime(data_ora[i], "%Y/%m/%d %H:%M").hour
                    #print "ora_letta", ora_letta
                    #check if we are at the end of the file
                    try: 
                        check=data_ora[i+1]
                    except:
                        check= "1900/01/01 00:00"
                        print "END FILE"
                    ########################################
                    # 00:00 two ZTD value
                    ########################################
                    if ora_letta==0 and data_ora[i]==check:
                        data_ora_ok.append(data_ora[i])
                        #print "scritta",data_ora[i]
                        #ZTD_ok.append((ZTD[i]+ZTD[i+1])/2)
                        ZTD_ok.append(ZTD[i])
                        #SQM_ZTD_ok.append((SQM_ZTD[i]+SQM_ZTD[i+1])/2)
                        SQM_ZTD_ok.append(SQM_ZTD[i])
                        i+=1
                        j+=1
                    ########################################
                    # 00:00 only one ZTD value
                    ########################################
                    else:    
                        data_ora_ok.append(data_ora[i])
                        #print "scritta",data_ora[i]
                        ZTD_ok.append(ZTD[i])
                        SQM_ZTD_ok.append(SQM_ZTD[i])
                        j+=1
                ########################################
                # lack of data
                ########################################
                elif data_letta > data_attesa:
                    if pippo==1:
                        quit()
                    ore,minuti,secondi = convert_timedelta(delta)
                    #print delta, ore
                    to_add = ore / intervallo
                    #print to_add
                    k=1
                    while k <= to_add:
                        #print k
                        da_scrivere=datetime.strptime(data_ora_ok[j-1], "%Y/%m/%d %H:%M")+ timedelta(hours=intervallo)
                        #print da_scrivere
                        data_ora_ok.append(da_scrivere.strftime("%Y/%m/%d %H:%M"))
                        ZTD_ok.append(99999)
                        SQM_ZTD_ok.append(99999)
                        k+=1
                        j+=1
                    #print i,j
                    #print "letta",data_letta
                    #print "attesa",data_attesa
                    #print "OK fine ciclo i=",i
                    data_ora_ok.append(data_ora[i])
                    #print "scritta",data_ora[i]
                    ZTD_ok.append((ZTD[i]+ZTD[i+1])/2)
                    SQM_ZTD_ok.append((SQM_ZTD[i]+SQM_ZTD[i+1])/2)
                    j+=1
                    #pippo=1
                else:
                    print "comportamento strano"
                    quit()
            i+=1


        



        # Open a cursor to perform database operations
        i=0
        #print len(data_ora_ok)
        while i<len(data_ora_ok):
            #print id_stazione[i]
            query="INSERT INTO ztd.dati_with_null(id_station, time, ztd, sqm_ztd) VALUES ('%s', '%s', %f , %f);" %(id_stazione,data_ora_ok[i], ZTD_ok[i], SQM_ZTD_ok[i])
            #print i,query
            try:
                cur.execute(query)
            except:
                print "violazione chiave primaria", query
            i+=1
 
    else:
        print "lack of data possible"
        data_ora_ok=[]
        ZTD_ok=[]
        SQM_ZTD_ok=[]
        i=0
        j=1
        while i <len(ZTD):
            if i==0:
                data_ora_ok.append(data_ora[i])
                ZTD_ok.append(ZTD[i])
                SQM_ZTD_ok.append(SQM_ZTD[i])
            else :
                data_letta=datetime.strptime(data_ora[i], "%Y/%m/%d %H:%M")
                #print i,j
                #print "i letta",data_letta
                #print "j letta", datetime.strptime(data_ora_ok[j-1], "%Y/%m/%d %H:%M")
                #print data_ora_ok
                data_attesa=datetime.strptime(data_ora_ok[j-1], "%Y/%m/%d %H:%M")+ timedelta(hours=intervallo)
                #print "attesa",data_attesa
                #quit()
                delta=data_letta - data_attesa
                #print delta
                ########################################
                #normally we read a ZTD every two hours
                ########################################
                if data_letta==data_attesa:
                    ora_letta=datetime.strptime(data_ora[i], "%Y/%m/%d %H:%M").hour
                    #print "ora_letta", ora_letta
                    #check if we are at the end of the file
                    try: 
                        check=data_ora[i+1]
                    except:
                        check= "1900/01/01 00:00"
                        print "END FILE"
                    ########################################
                    # 00:00 two ZTD value
                    ########################################
                    if ora_letta==0 and data_ora[i]==check:
                        data_ora_ok.append(data_ora[i])
                        print "scritta",data_ora[i]
                        #ZTD_ok.append((ZTD[i]+ZTD[i+1])/2)
                        ZTD_ok.append(ZTD[i])
                        #SQM_ZTD_ok.append((SQM_ZTD[i]+SQM_ZTD[i+1])/2)
                        SQM_ZTD_ok.append(SQM_ZTD[i])
                        i+=1
                        j+=1
                    ########################################
                    # 00:00 only one ZTD value
                    ########################################
                    else:    
                        data_ora_ok.append(data_ora[i])
                        #print "scritta",data_ora[i]
                        ZTD_ok.append(ZTD[i])
                        SQM_ZTD_ok.append(SQM_ZTD[i])
                        j+=1
                ########################################
                # lack of data
                ########################################
                elif data_letta > data_attesa:
                    if pippo==1:
                        quit()
                    ore,minuti,secondi = convert_timedelta(delta)
                    print "Lack of data for %d hours" % ore
                    to_add = ore / intervallo
                    k=1
                    while k <= to_add:
                        #print k
                        da_scrivere=datetime.strptime(data_ora_ok[j-1], "%Y/%m/%d %H:%M")+ k*timedelta(hours=intervallo)
                        #print da_scrivere
                        #data_ora_ok.append(da_scrivere.strftime("%Y/%m/%d %H:%M"))
                        #ZTD_ok.append(99999)
                        #SQM_ZTD_ok.append(99999)
                        k+=1
                        #j+=1
                        #print i,j
                    data_attesa=datetime.strptime(data_ora[i], "%Y/%m/%d %H:%M")
                    #j+=1
                    #print i,j
                    #print "letta",data_letta
                    #print "attesa",data_attesa
                    #print "OK fine ciclo i=",i
                    data_ora_ok.append(data_ora[i])
                    #print "scritta",data_ora[i]
                    ZTD_ok.append((ZTD[i]+ZTD[i+1])/2)
                    SQM_ZTD_ok.append((SQM_ZTD[i]+SQM_ZTD[i+1])/2)
                    j+=1
                    #i=i-1
                    #pippo=1
                else:
                    print "comportamento strano"
                    quit()
            i+=1


        



        # Open a cursor to perform database operations
        i=0
        print len(data_ora_ok)
        while i<len(data_ora_ok):
            #print id_stazione[i]
            query="INSERT INTO ztd.dati(id_station, time, ztd, sqm_ztd) VALUES ('%s', '%s', %f , %f);" %(id_stazione,data_ora_ok[i], ZTD_ok[i], SQM_ZTD_ok[i])
            #print i,query
            try:
                cur.execute(query)
            except:
                print "violazione chiave primaria", query
            i+=1
    
    
    # Close communication with the database
    cur.close()
    conn.close()
    
    
if __name__ == "__main__":
    main()
