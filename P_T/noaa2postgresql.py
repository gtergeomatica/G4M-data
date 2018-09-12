#!/usr/bin/env python
# -*- coding: utf-8 -*-
#Gter copyleft 
#Roberto Marzocchi

import sys
import os
import math
import time
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

    #nomefile1 = "FRA_31102011.txt"

    id_stazione=[]
    data_ora=[]
    T=[]
    P=[]
    P_mare=[]


    from time import strftime


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
        if i>0:
            #print i
            a = line.split()
            id_stazione.append((a[0]))
            data_temp=time.strptime(a[2], "%Y%m%d%H%M") 
            data_ora.append(strftime("%Y/%m/%d %H:%M", data_temp))
            #if (check[i-1]=='*'):
            try:
                T.append((float(a[21])+459.67)/1.8) #conversione da Fahrenheit a Kelvin K = (°F + 459,67) / 1,8
            except: 
                T.append(99999)
            try:
                P.append(float(a[25])) # P in quota 
            except:
                P.append(99999)
            try:
                P_mare.append(float(a[23])) # P al livello del mare
            except:
                P_mare.append(99999)
            j=0
            #if i==1:
            #    while j < len(a):
            #        print j,(a[j])
            #        j+=1
        i+=1
        
        

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
    print len(T)
    while i<len(T):
        #print id_stazione[i]
        query="INSERT INTO noaa.data_p_t(id_station, time, \"T\", \"P\", \"P_mare\") VALUES ('%s', '%s',%f , %f, %f);" %(id_stazione[i],data_ora[i], T[i], P[i], P_mare[i])
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
