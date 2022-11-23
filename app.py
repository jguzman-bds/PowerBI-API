import json
import pandas as pd
import requests
import streamlit as st

auth_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IjJaUXBKM1VwYmpBWVhZR2FYRUpsOGxWMFRPSSIsImtpZCI6IjJaUXBKM1VwYmpBWVhZR2FYRUpsOGxWMFRPSSJ9.eyJhdWQiOiJodHRwczovL2FuYWx5c2lzLndpbmRvd3MubmV0L3Bvd2VyYmkvYXBpIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvOGQzMmE0MjgtOThlOS00YTE0LTgyNjItOTQ0ZDJmNTIwYjc4LyIsImlhdCI6MTY2OTIyMTQ3MCwibmJmIjoxNjY5MjIxNDcwLCJleHAiOjE2NjkyMjY4MjYsImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBVlFBcS84VEFBQUFtNFFhNHhGcE1yT1RTMzJVd0RXeWJIczNhdWNvRjlvY29TWGczNThWL3VFWWw1WkNYTkVoRWJCTHh1R29JalJQN3d5VndSTzdrR1UyK2FqTDg0SlRLYjNDbVlGSWFJT1NaUHAzbnEwTWtLRT0iLCJhbXIiOlsicHdkIiwibWZhIl0sImFwcGlkIjoiMDRiMDc3OTUtOGRkYi00NjFhLWJiZWUtMDJmOWUxYmY3YjQ2IiwiYXBwaWRhY3IiOiIwIiwiZmFtaWx5X25hbWUiOiJHdXptYW4iLCJnaXZlbl9uYW1lIjoiSm9oYW4iLCJpcGFkZHIiOiI4MS40NS40MC4xMSIsIm5hbWUiOiJKb2hhbiBHdXptYW4iLCJvaWQiOiJjMjQyMzczYS02ZWFiLTRlZGMtODIxOC03MzRmNDI3MTg2MjciLCJwdWlkIjoiMTAwMzIwMDIzMjMxMDc0QSIsInJoIjoiMC5BVWdBS0tReWplbVlGRXFDWXBSTkwxSUxlQWtBQUFBQUFBQUF3QUFBQUFBQUFBQklBTncuIiwic2NwIjoidXNlcl9pbXBlcnNvbmF0aW9uIiwic3ViIjoiVDRoblozSU9ROWZBVF8yel9ubldfc0JCSWJrc0NTbmp1cm5JS09LRjlKYyIsInRpZCI6IjhkMzJhNDI4LTk4ZTktNGExNC04MjYyLTk0NGQyZjUyMGI3OCIsInVuaXF1ZV9uYW1lIjoiai5ndXptYW5AYmRpZ2l0YWxzb2x1dGlvbnMuY29tIiwidXBuIjoiai5ndXptYW5AYmRpZ2l0YWxzb2x1dGlvbnMuY29tIiwidXRpIjoiMGxuYUxhME5oMEtNMHNsU0loRzhBQSIsInZlciI6IjEuMCIsIndpZHMiOlsiYjc5ZmJmNGQtM2VmOS00Njg5LTgxNDMtNzZiMTk0ZTg1NTA5Il0sInhtc19jYyI6WyJDUDEiXX0.eeosEN9KcQqDhly-ieBr855E0yM_iOuadKtOHmblmMnbcD6OJJ00CA0b1gO_z_j_95x-aajhSxTmeZJqS3gyBFlxmnQQaxzTCDnFSWy83lBj5F4OTdZHROSQwwpatmfMgxg7LBJnm_JBPZYgbaEezvPmgXemsgKdFmAM8Wjusg-wmhgYVtEI7H1VJxYuSCoHjbfuiXv8zJptuD5C6s_s3Z8JsspJunHaHP0jLw1gtMMNZZwKknH39w3mMtZTvyhmiP0bkNnw_eAtTfbIpjpEJO7s56dg3GNqvEaMLxKTaUGJ7nfARcAbz5DPvUhWHpZMrLe63PnN4MQJabZq5DC5LA"
dataset = "bc8741ce-3063-4647-9b1e-34c5a855fc8e"

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
st.write(df)
