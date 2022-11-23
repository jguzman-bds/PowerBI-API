import json
import pandas as pd
import requests

# Post Request method to run the query against the XMLA dataset endpoint returning a pandas dataframe parsed from json response 
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
        return res
    except requests.exceptions.HTTPError as ex:
        print(ex)
    except Exception as e:
        print(e)
        
# Executions to make it happen
#auth_token = get_auth_token(power_bi_client_id, power_bi_username, power_bi_password)

# Write your DAX  query inside the query parameter. There you have an example.
query = "EVALUATE VALUES('airline_passenger_satisfaction'[Age])"

# Capture dataframe in df 
df = post_dax_query(query, auth_token, dataset)

# Have fun with your df
print(df)
