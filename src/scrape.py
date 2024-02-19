import ssl
ssl._create_default_https_context = ssl._create_unverified_context
import urllib.request
import requests
import unicodedata
import pandas as pd
import os
import numpy as np

def get_stations(files):
    foo = []

    for i in files:
      foo.append(i.split('_')[1].split('_')[0].replace('.', ''))

    foo = list(dict.fromkeys(foo))
    return foo

def open_df(stations, files):
    fileNames = []

    for i in stations:
      tmp = []
      for j in files:
        if i in j:
          tmp.append(pd.read_excel('../data/'+j))
      fileNames.append(tmp)

    return fileNames

def pre_processing(fileNames):
    for i in range(len(fileNames)):
      for j in range(len(fileNames[i])):
        if 'Unnamed: 2' in fileNames[i][j]:
          fileNames[i][j].columns = fileNames[i][j].iloc[0]
          fileNames[i][j] = fileNames[i][j].drop(fileNames[i][j].index[[0,1]])
          fileNames[i][j] = fileNames[i][j].dropna(axis=1, how='all')
        else:
          fileNames[i][j] = fileNames[i][j].drop(fileNames[i][j].index[[0]])
          fileNames[i][j] = fileNames[i][j].dropna(axis=1, how='all')
        fileNames[i][j].rename(columns={fileNames[i][j].columns[0]: 'Timestamp'},inplace=True)
        if pd.isna(fileNames[i][j].columns[-1]): fileNames[i][j] = fileNames[i][j].drop([np.nan], axis=1)

    conc = []

    for i in fileNames:
      conc.append(pd.concat(i))

    return conc

def to_csv(conc, stations):
    for i in range(len(conc)):
      conc[i].to_csv('../final_data/' + stations[i]+'.csv', sep=';', index=False, encoding='utf-8')


def remove_accents(input):
  nfkd_form = unicodedata.normalize('NFKD', input)
  return u''.join([c for c in nfkd_form if not unicodedata.combining(c)])

def get_FileNames(link): 
    try:
        fileNames = []
        my_request = urllib.request.urlopen(link)
        my_HTML = my_request.read().decode("utf8")
        select = my_HTML.split('\"text\">')[1].split('href=\"')
        for i in select:
            aux = i.split('\">')[0]
            if 'http' not in aux and '<em>' not in aux and 'target' not in aux and 'responsabilidades' not in aux: fileNames.append(aux)
        return fileNames
    except:
        print('Erro')

def main():
    fileNames = get_FileNames('http://www.feam.br/component/content/article/15/2030-congonhas')

    for i in fileNames:
        url = 'http://www.feam.br' + i
        response = requests.get(url)
        name = i.split('congonhas/')[1]
        name = remove_accents(name)
        open(f'../data/{name}', "wb").write(response.content)
    
    files = os.listdir("../data/")
    stations = get_stations(files)
    df = open_df(stations, files)
    df = pre_processing(df)
    to_csv(df, stations)

if __name__ == "__main__":
    main()
