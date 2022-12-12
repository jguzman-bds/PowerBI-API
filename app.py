from azure.identity import ClientSecretCredential, InteractiveBrowserCredential, UsernamePasswordCredential
import requests

scope = st.secrets["scope"]

client_id = st.secrets["client_id"]

username = st.secrets["username"]
password = st.secrets["password"]

tenant_id = st.secrets["tenant_id"]
client_secret = st.secrets["client_secret"]

authority_url = st.secrets["authority_url"]

scope2 = [scope]


import msal


def get_token_username_password(scope2):
    app = msal.PublicClientApplication(client_id, authority=authority_url)
    result = app.acquire_token_by_username_password(username=username,password=password,scopes=scope2)
    if 'access_token' in result:
        return(result['access_token'])
    else:
        print('Error in get_token_username_password:',result.get("error"), result.get("error_description"))
        
token_string = get_token_username_password(scope2)
auth_token = token_string

##

import json
import pandas as pd
import requests


dataset = '1253097e-29d1-4082-87e2-66f45a2ceb95'


def post_dax_query(query, auth_token, dataset):
    try: 
        url= "https://api.powerbi.com/v1.0/myorg/datasets/"+dataset+"/executeQueries"        
        body = {"queries": [{"query": query}], "serializerSettings": {"incudeNulls": "true"}}
        headers={'Content-Type': 'application/json', "Authorization": "Bearer {}".format(auth_token)}
        res = requests.post(url, data = json.dumps(body), headers = headers)
        #get columns from json response - keys from dict
        columnas = list(res.json()['results'][0]['tables'][0]['rows'][0].keys())
        #get the number of rows to loop data
        filas = len(res.json()['results'][0]['tables'][0]['rows'])        
        #get data from json response - values from dict
        datos = [list(res.json()['results'][0]['tables'][0]['rows'][n].values()) for n in range(filas-1)]
        #build a dataframe from the collected data
        df = pd.DataFrame(data=datos, columns=columnas)
        print(df.head())
        return response_to_pandas(res)
        #return res
    except requests.exceptions.HTTPError as ex:
        print(ex)
    except Exception as e:
        print(e)
        
        
def response_to_pandas(res):
    data = res.json()
    pandasdf = pd.json_normalize(data['results'][0]['tables'], record_path =['rows'])
    return pandasdf


query_TIPORECO = """// DAX Query
DEFINE
  VAR __DS0FilterTable = 
    FILTER(
      KEEPFILTERS(VALUES('ConsultaReconocimientoElemento'[FECHARECO])),
      AND(
        'ConsultaReconocimientoElemento'[FECHARECO] >= DATE(2021, 1, 1),
        'ConsultaReconocimientoElemento'[FECHARECO] < DATE(2022, 1, 1)
      )
    )

  VAR __DS0Core = 
    SUMMARIZECOLUMNS(
      'ConsultaReconocimientoElemento'[TIPORECO],
      __DS0FilterTable,
      "DistinctCountID", CALCULATE(DISTINCTCOUNT('ConsultaReconocimientoElemento'[ID]))
    )

  VAR __DS0BodyLimited = 
    TOPN(1002, __DS0Core, [DistinctCountID], 0, 'ConsultaReconocimientoElemento'[TIPORECO], 1)

EVALUATE
  __DS0BodyLimited

ORDER BY
  [DistinctCountID] DESC, 'ConsultaReconocimientoElemento'[TIPORECO]
"""

df_TIPORECO = post_dax_query(query_TIPORECO, auth_token, dataset)

##

import matplotlib.pyplot as plt
import numpy as np
import math


values_TIPORECO = df_TIPORECO['[DistinctCountID]']
labels_TIPORECO = df_TIPORECO['ConsultaReconocimientoElemento[TIPORECO]']

p_tipo_reco, a_tipo_reco = plt.subplots()

l = a_tipo_reco.pie(values_TIPORECO, startangle=15)

for label, t in zip(labels_TIPORECO, l[1]):
    x, y = t.get_position()
    angle = int(math.degrees(math.atan2(y, x)))
    ha = "left"
    va = "bottom"

    if angle > 90:
        angle -= 180

    if angle < 0:
        va = "top"

    if -45 <= angle <= 0:
        ha = "right"
        va = "bottom"

    plt.annotate(label, xy=(x,y), rotation=angle, ha=ha, va=va, size=8)

p_tipo_reco.set_edgecolor('white')

a_tipo_reco.set_title("Tipos de reconocimiento");
st.pyplot(p_tipo_reco)
