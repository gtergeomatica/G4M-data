#!/usr/bin/env python
#Gter copyleft 
#Roberto Marzocchi

import sys
import os
import math
import atexit
import shutil,re,glob

space = re.compile(r'\s+')
multiSpace = re.compile(r"\s\s+") 

import getopt


def main():
    nomefile1 = ''
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hi:n", ["help", "ifile="])
    except getopt.GetoptError:
        print 'ztd2postgresql.py -i <nomefile1>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'ztd2postgresql.py -i <nomefile1>'
            sys.exit()
        elif opt in ("-i", "--ifile"):
            nomefile1 = arg
            
    if nomefile1=='':
        print 'ERROR: specify an input file'
        sys.exit()
    print 'Input file is ', nomefile1
    
    #quit()


    #nomefile1 = "stn_ITA.txt"

    id_stazione=[]
    desc=[]
    country=[]
    lat=[]
    lon=[]
    ele=[]




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
        if i>1:
            #print i
            #a = line.split()
            id_stazione.append(line[0:6])
            desc.append(line[13:43].rstrip())
            country.append(line[44:126].rstrip())
            lat.append(float(line[127:135].rstrip()))
            lon.append(float(line[136:146].rstrip()))
            ele.append(float(line[147:].rstrip()))
        i+=1
    print desc
        
    import psycopg2


    # Connect to an existing database
    sys.path.append(os.path.abspath("../"))
    from credenziali import *
    conn = psycopg2.connect(host=ip, dbname=db, user=user, password=pwd, port=port)
    #autocommit
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # Open a cursor to perform database operations
    i=0
    print len(lat)
    while i<len(lat):
        #print id_stazione[i]
        query="INSERT INTO noaa.stations_p_t(id_station, descr, country, ele, geom) VALUES ('%s', '%s','%s' , %f, ST_SetSRID(ST_MakePoint(%f,%f,%f),4326));" %(id_stazione[i],desc[i], country[i], ele[i], lon[i],lat[i],ele[i])
        #print i,query
        try:
            cur.execute(query)
        except:
            print "violazione chiave primaria", query
        i+=1

    # Make the changes to the database persistent
    #conn.commit()

    # Close communication with the database
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
