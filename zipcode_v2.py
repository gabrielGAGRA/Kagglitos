import re
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.distance import distance
import brazilcep as pcep

import time
from brazilcep.exceptions import BrazilCEPException, CEPNotFound

InvalidCEP = []

def getAdress(cep):
    global InvalidCEP
    endereco = None
    for attempt in range(3): 
        try: 
            endereco = pcep.get_address_from_cep(cep)
            return endereco
        except CEPNotFound:
            InvalidCEP.append(cep)
            break 
        except BrazilCEPException as e:
            if "429" in str(e):
                print("Rate limit hit, sleeping...")
                time.sleep(10 ** attempt)
            else:
                print("Other BrazilCEPException occurred.")
                return None
    return endereco
    
def getCoordinates(endereco):
    # recebe endereco em rua, bairro e cidade
    rua = endereco['logradouro']
    bairro = endereco['bairro']
    cidade = endereco['cidade']
    # para evitar problemas com cidades menores sem rua e bairro, checamos se eles existem
    if rua and bairro:
        rua = f"{rua[0]}. {rua[3:]}"
        coordenadas = geolocator.geocode(f"{rua}, {bairro}-{cidade}")
    else:
        coordenadas = geolocator.geocode(f"{cidade}")
    return coordenadas

DATA_URL = r"C:\Users\gabri\Documents\PROJETOS\PY\PJ_Code\DE\Data\relacao_total.csv"
df = pd.read_csv(DATA_URL)  

# configura o geolocator
geolocator = Nominatim(user_agent="geolocalização")

# lista (coluna) das distancias
distanciasList = []

for cepCliente, cepVendedor in zip(df['customer_zip_code_prefix'], df['seller_zip_code_prefix']):
    # preenche o prefixo do cep com zeros a direita ate ter 8 digitos
    cepCliente = str(cepCliente).ljust(8, '0')
    cepVendedor = str(cepVendedor).ljust(8, '0')
    
    #tenta obter endereco pelo CEP
    enderecoCliente = getAdress(cepCliente)
    print(InvalidCEP)
    InvalidCEP = []
    enderecoVendedor = getAdress(cepVendedor)
    print(InvalidCEP)
    
    # obtem distancia a partir do endereco
    coordenadasCustomer = getCoordinates(enderecoCliente) 
    coordenadasSeller = getCoordinates(enderecoVendedor)
    distancia = distance(coordenadasCustomer, coordenadasSeller).km
    
    # adiciona a distancia a coluna de distancias
    distanciasList.append(distancia)

# cria uma coluna no dataframe com todas as distancias 
df['distancia'] = distanciasList
# salva o csv
df.to_csv('dadosLimpos.csv', index=False)