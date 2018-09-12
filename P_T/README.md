# Gestione dati di Pressione e Temperatura

- Cartela noaa_data
- 2 script per leggere i dati e inserirli nelle opportune tabelle del DB:
  - ./noaa2postgresql.py -i noaa_data/*nome_file* 
  - ./noaa2postgresql_stazioni.py -i noaa_data/*nome_file*
- file *credenziali.py* con le credenziali per la connessione al database (nell'esempio sono quelle valide su rete interna GTER), possono essere mutuate per ogni DB

----

## Conversioni di unità di misura

Il file letti dal NOAA hanno le seguenti unità di misura:
- Temperatura T in gradi Fahrenheit --> Per cui effettuiamo la conversione in gradi kelvin con la seguente formula: K = (°F + 459,67) / 1,8
- Presssione P in ettoPascal (hPA) per cui non va convertita
