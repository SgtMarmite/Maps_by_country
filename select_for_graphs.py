import sqlite3
import pandas as pd   
import re
import numpy as np
import time
from tqdm import tqdm
from geopy.geocoders import Nominatim

cnx = sqlite3.connect('reality.db')
prodeje = pd.read_sql_query('''
select
	odkaz,
	cena,
	poznamkaCena,
	typBytu,
	kraj,
	aktualizace,
	stavba,
	stavobjektu,
	vlastnictvi,
	umisteniObjektu,
	podlazi,
	uzitnaPlocha,
	balkon,
	sklep,
	voda,
	topeni,
	plyn,
	odpad,
	elektrina,
	doprava,
	energetickaNarocnost,
    parkovani,
	lokace,
	strftime('%m', t_timeOfImport) as month_num
from prodej
order by odkaz, month_num asc
''', cnx)

prodeje.month_num = pd.to_numeric(prodeje.month_num, errors='coerce')

pronajmy = pd.read_sql_query('''
select
	odkaz,
	cena,
	poznamkaCena,
	typBytu,
	kraj,
	aktualizace,
	stavba,
	stavobjektu,
	vlastnictvi,
	umisteniObjektu,
	podlazi,
	uzitnaPlocha,
	balkon,
	sklep,
	voda,
	topeni,
	plyn,
	odpad,
	elektrina,
	doprava,
	energetickaNarocnost,
    parkovani,
	lokace,
	strftime('%m', t_timeOfImport) as month_num
from pronajem
order by odkaz, month_num asc
''', cnx)

cnx.close()

def drop_empty():
    prodeje['cena'].replace('', np.nan, inplace=True)
    prodeje.dropna(subset=['cena'], inplace=True)
    pronajmy['cena'].replace('', np.nan, inplace=True)
    pronajmy.dropna(subset=['cena'], inplace=True)  

def provize(column):
    x = re.search('včetně provize', column)
    y = re.search('\+ provize RK', column)
    if x or y is not None:
        return 1
    
def cenaKc(column):
    column = column.replace(' ','')
    pattern = re.compile(r'\d{2,8}Kč')
    try:
        x = pattern.match(column).group(0)
        x = x.replace('Kč', '')
    except:
        x = ''
    return x
    
def trida(column):
    column = column.replace(' ','')
    pattern = re.compile(r'[A-Z]-')
    try:
        x = pattern.search(column).group(0)
        x = x[:1]
    except:
        x = ''
    return x

def podlazi(column):
    pattern = re.compile(r'^\d.')
    try:
        x = pattern.search(column).group(0)
        x = re.search(r'\d+',x).group()
    except:
        x = ''
    return x

def uzitnaPlocha(column):
    try:
        x = re.search(r'\d+',column).group()
    except:
        x = ''
    return x

def parkovani(column):
    if column != '':
        x = '1'
    else:
        x = '0'
    return x

def isnotempty(column):
    if column != '':
        x = '1'
    else:
        x = '0'
    return x

def getLatLong(address):
    
    try:
        geolocator=Nominatim(domain='localhost:8080', scheme='http')
        location = geolocator.geocode(address)
        long = location.longitude
        lat = location.latitude
        
    except:
        
        lat, long = None, None
        time.sleep(0.1)
    
    return lat, long

drop_empty()
prodeje = prodeje.reset_index()
pronajmy = pronajmy.reset_index()

prodeje['kraj'] = prodeje['kraj'].replace('Pardubický', 'Pardubický kraj')
pronajmy['kraj'] = pronajmy['kraj'].replace('Pardubický', 'Pardubický kraj')

prodeje['vcetne_provize'] = prodeje['cena'].apply(provize)
pronajmy['vcetne_provize'] = pronajmy['cena'].apply(provize)
prodeje['cena_Kc'] = prodeje['cena'].apply(cenaKc)
pronajmy['cena_Kc'] = pronajmy['cena'].apply(cenaKc)
prodeje['energeticka_trida'] = prodeje['energetickaNarocnost'].apply(trida)
pronajmy['energeticka_trida'] = pronajmy['energetickaNarocnost'].apply(trida)
prodeje['parkovani'] = prodeje['parkovani'].apply(parkovani)
pronajmy['parkovani'] = pronajmy['parkovani'].apply(parkovani)
prodeje['podlazi_cislo'] = prodeje['podlazi'].apply(podlazi)
pronajmy['podlazi_cislo'] = pronajmy['podlazi'].apply(podlazi)
prodeje['uzitnaPlocha_cislo'] = prodeje['uzitnaPlocha'].apply(podlazi)
pronajmy['uzitnaPlocha_cislo'] = pronajmy['uzitnaPlocha'].apply(podlazi)
prodeje['balkon_anone'] = prodeje['balkon'].apply(isnotempty)
pronajmy['balkon_anone'] = pronajmy['balkon'].apply(isnotempty)
prodeje['sklep_anone'] = prodeje['sklep'].apply(isnotempty)
pronajmy['sklep_anone'] = pronajmy['sklep'].apply(isnotempty)

prodeje['lat'], prodeje['long'] = None, None
pronajmy['lat'], pronajmy['long'] = None, None

#prodeje = prodeje.sample(frac=0.1, replace=True)
#prodeje = prodeje.reset_index()

for i in enumerate(tqdm(prodeje.index)):

    prodeje['lat'][i[0]], prodeje['long'][i[0]] = getLatLong(prodeje['lokace'][i[0]])
    
    if ((prodeje['lat'][i[0]] == None) & (prodeje['long'][i[0]] == None)):
        try:
            prodeje['lokace'][i[0]] = re.search(r'.{10,50} -',prodeje['lokace'][i[0]]).group()
            prodeje['lokace'][i[0]] = prodeje['lokace'][i[0]].replace(' -', '')
            
            prodeje['lat'][i[0]], prodeje['long'][i[0]] = getLatLong(prodeje['lokace'][i[0]])
        except:
            pass

    if ((prodeje['lat'][i[0]] == None) & (prodeje['long'][i[0]] == None)):
        try:
            prodeje['lokace'][i[0]] = prodeje['lokace'][i[0]].replace('ulice ', '')
            prodeje['lat'][i[0]], prodeje['long'][i[0]] = getLatLong(prodeje['lokace'][i[0]])
        except:
            pass

    if ((prodeje['lat'][i[0]] == None) & (prodeje['long'][i[0]] == None)):
        try:
            prodeje['lokace'][i[0]] = prodeje['lokace'][i[0]] + ', Česká republika'
            prodeje['lat'][i[0]], prodeje['long'][i[0]] = getLatLong(prodeje['lokace'][i[0]])
        except:
            pass

    if ((prodeje['lat'][i[0]] == None) & (prodeje['long'][i[0]] == None)):
        try:
            prodeje['lokace'][i[0]] = re.search(r'^(.*?),',prodeje['lokace'][i[0]]).group()
            prodeje['lokace'][i[0]] = prodeje['lokace'][i[0]].replace(',', '')
            prodeje['lat'][i[0]], prodeje['long'][i[0]] = getLatLong(prodeje['lokace'][i[0]])
        except:
            print('Result not found in Nominatim DB')
        
for i in enumerate(tqdm(pronajmy.index)):

    pronajmy['lat'][i[0]], pronajmy['long'][i[0]] = getLatLong(pronajmy['lokace'][i[0]])
    
    if ((pronajmy['lat'][i[0]] == None) & (pronajmy['long'][i[0]] == None)):
        try:
            pronajmy['lokace'][i[0]] = re.search(r'.{10,50} -',pronajmy['lokace'][i[0]]).group()
            pronajmy['lokace'][i[0]] = pronajmy['lokace'][i[0]].replace(' -', '')
            
            pronajmy['lat'][i[0]], pronajmy['long'][i[0]] = getLatLong(pronajmy['lokace'][i[0]])
        except:
            pass

    if ((pronajmy['lat'][i[0]] == None) & (pronajmy['long'][i[0]] == None)):
        try:
            pronajmy['lokace'][i[0]] = pronajmy['lokace'][i[0]].replace('ulice ', '')
            pronajmy['lat'][i[0]], pronajmy['long'][i[0]] = getLatLong(pronajmy['lokace'][i[0]])
        except:
            pass

    if ((pronajmy['lat'][i[0]] == None) & (pronajmy['long'][i[0]] == None)):
        try:
            pronajmy['lokace'][i[0]] = pronajmy['lokace'][i[0]] + ', Česká republika'
            pronajmy['lat'][i[0]], pronajmy['long'][i[0]] = getLatLong(pronajmy['lokace'][i[0]])
        except:
            pass

    if ((pronajmy['lat'][i[0]] == None) & (pronajmy['long'][i[0]] == None)):
        try:
            pronajmy['lokace'][i[0]] = re.search(r'^(.*?),',pronajmy['lokace'][i[0]]).group()
            pronajmy['lokace'][i[0]] = pronajmy['lokace'][i[0]].replace(',', '')
            pronajmy['lat'][i[0]], pronajmy['long'][i[0]] = getLatLong(pronajmy['lokace'][i[0]])
        except:
            print('Result not found in Nominatim DB')


prodeje = prodeje[['odkaz','typBytu','kraj','stavba','stavObjektu','vlastnictvi','umisteniObjektu','voda','topeni','plyn','odpad','elektrina','lokace','vcetne_provize','energeticka_trida','podlazi_cislo','uzitnaPlocha_cislo','parkovani','balkon_anone','sklep_anone','lat','long','cena_Kc']]
pronajmy = pronajmy[['odkaz','typBytu','kraj','stavba','stavObjektu','vlastnictvi','umisteniObjektu','voda','topeni','plyn','odpad','elektrina','lokace','vcetne_provize','energeticka_trida','podlazi_cislo','uzitnaPlocha_cislo','parkovani','balkon_anone','sklep_anone','lat','long','cena_Kc']]

prodeje.to_csv('prodeje_graphs.csv')
pronajmy.to_csv('pronajmy_graphs.csv')

prodeje_problemove = prodeje[prodeje['lat'].isna()]
pronajmy_problemove = pronajmy[pronajmy['lat'].isna()]

import seaborn as sns
import matplotlib.pyplot as plt

plt.figure(figsize = (10,6))
sns.scatterplot(prodeje.long, prodeje.lat, hue=prodeje.kraj, legend=None)
ax=plt.gca()
plt.show()

plt.figure(figsize = (10,6))
sns.scatterplot(pronajmy.long, pronajmy.lat, hue=pronajmy.kraj, legend=None)
ax=plt.gca()
plt.show()