# G4M - data
Repository of the research project GNSS for meteorology (G4M) to download data from existing monitoring networks and store in a PostgreSQL/PostGIS DB.

The repository use the following structure: 

- ZTD : folder with the script to insert Zenith Total Delay (ZTD) in the PostgreSQL/PostGIS geoDB from folder coming from RENAG-DB ftp
- P_T: folder with the script to insert Pressure P and Temperature T in the PostgreSQL/PostGIS geoDB
- SQL file to create the geoDB tables

They use the credenziali.py file which is ignored

′′′
ip='localhost'   
db='##############'
user='########'
pwd='#########'
port='5432'

schema='meteognss_p_t' #example
′′′

# NOTE
The README.md of single folder are actually in Italian (Translation will be soon available)

#LINK
G4M is a project of Laboratory of Geodesy, Geomatics and GIS of the University of Genoa [Link](http://www.dicca.unige.it/geomatica/ricerca/)
