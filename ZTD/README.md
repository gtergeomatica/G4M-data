# Gestione dati ZTD

- Cartella data con i dati delle stazioni (per il 2011)
- script per leggere i dati e inserirli nella tabella del DB:
  - ./ztd2postgresql.py -i data/2011_biorario/net*X*/*nome_file* [-n FALSE] dove il parametro opzionale -n specifica se inserire o meno 99999 nel DB  quando i dati sono nulli (di default il parametro è impostato su TRUE)
- file *credenziali.py* con le credenziali per la connessione al database (nell'esempio sono quelle valide su rete interna GTER), possono essere mutuate per ogni DB
- ztd_iterativo.sh script bash per lanciare in maniera iterativo lo script python di cui sopra sulle varie cartelle (e' specifico per i dati presenti su questo repository e ovviamente potrebbe essere da modificare opportunamente, ma è molto comodo per non doversi inserire i dati di ciascuna stazione a mano) 
