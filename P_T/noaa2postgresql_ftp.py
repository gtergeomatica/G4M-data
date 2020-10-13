#!/usr/bin/env python
# -*- coding: utf-8 -*-
#Gter copyleft 
#Roberto Marzocchi, Lorenzo Benvenuto

import sys
import os
import math
import time
import atexit
import shutil,re,glob

import urllib

import gzip

space = re.compile(r'\s+')
multiSpace = re.compile(r"\s\s+") 

import getopt


import psycopg2




def main():
    
    
    year = ''
    try:
        opts, args = getopt.getopt(sys.argv[1:], "ha:n", ["help", "ayear="])
    except getopt.GetoptError:
        print 'noaa2postgresql.py -a <year>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'noaa2postgresql.py -i <year>'
            sys.exit()
        elif opt in ("-a", "--ayear"):
            year = arg
            
    if year=='':
        print 'ERROR: specify an year'
        sys.exit()
    print 'Download data for the ', year
    #quit()
    
    

    #nomefile1 = "FRA_31102011.txt"
    percorso_FTP='ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-lite/'
    
    
    # Connect to an existing database
    sys.path.append(os.path.abspath("../"))
    from credenziali import *
    conn = psycopg2.connect(host=ip, dbname=db, user=user, password=pwd, port=port)
    #autocommit
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur2 = conn.cursor()




    #leggo il codice delle stazioni presenti sul DB
    # Open a cursor to perform database operations
    query="SELECT id_station,descr, country FROM {}.stations_p_t;".format(schema)
    cur.execute(query)  
    
    while True:
        row=cur.fetchone()       
        if row == None:
            break
        cod=row[0]
        url_filename='%s%s/%s-99999-%s.gz' % (percorso_FTP,year,cod,year)
        filename='%s-99999-%s' % (cod,year)
        zipname='%s.gz' % filename
        
        print url_filename
        print filename
        print zipname
        print '*****************'
        try:
            urllib.urlretrieve(url_filename,zipname)
            urllib.urlcleanup() 
            
            zip_ref = gzip.open(zipname, 'rb')
            file_content= zip_ref.read()
            f1 = open(filename, 'w')      
            for line in file_content:
                f1.write(line)
            zip_ref.close()
            f1.close()
            
            #time.sleep(10)
            f1 = open(filename, 'r')
            print "Reading the file",filename
            #leggo le tre colonne dove nella prima c'e' il 2
            i=0
            n=0
            riga_prima=" "
            line= " "
            yyyy=[]
            mm=[]
            dd=[]
            hh=[]
            tt=[]
            pp=[]
            for riga in file(filename): 
                line = riga
                #print i
                #print line
                a = line.split()
                #print a
                yyyy.append(a[0])
                mm.append(a[1])
                dd.append(a[2])
                hh.append(a[3])
                ########################################################################################
                #read temperature (scaling factor NOAA = 10.0)
                if (a[4]=='-9999'):  # null value NOAA 
                    tt.append(99999)  # null value DICCA
                else:
                    tt.append(float(a[4])/10.0+273.15) #conversione da Celsius a Kelvin K = °C + 273.15 
                ########################################################################################
                #read pressure (scaling factor NOAA = 10.0)
                if (a[6]=='-9999'): # null value NOAA 
                    pp.append(99999) # null value DICCA 
                else:    
                    pp.append(float(a[6])/10.0)

            #print min(tt)
            #print max(tt)
            #print min(pp)
            #print max(pp)
            
            
              
            # Insert data in the DB
            i=0
            print "lunghezza file", len(tt)
            while i<len(tt):
                #print id_stazione[i]
                data='%s/%s/%s %s:00' %(yyyy[i],mm[i],dd[i],hh[i])
                query2="INSERT INTO noaa.data_p_t(id_station, time, \"T\", \"P_mare\") VALUES ('%s', '%s',%f , %f);" %(cod,data, tt[i], pp[i])
                #print i,query
                #print i
                try:
                    cur2.execute(query2)
                except:
                    print "violazione chiave primaria", query2
                i+=1
                
            os.remove(filename)
            os.remove(zipname)
        except:
            print "Non trovato file", filename
        
        
        
        
        
              


    #quit()
    
    
    
    
    # Make the changes to the database persistent
    #conn.commit()


    
        
        
        





    # Close communication with the database
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
