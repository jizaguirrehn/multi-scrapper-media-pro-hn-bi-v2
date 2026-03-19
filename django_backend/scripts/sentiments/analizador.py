import os
import requests
import numpy as np
import pandas as pd
import json
from dotenv import load_dotenv

class get_data:
    def __init__(self):
        load_dotenv()
        self.url = os.getenv("URL_DATABRICKS")
        self.token = os.getenv("DATABRICKS_TOKEN")

    def __create_tf_serving_json(self, data):
        return {'inputs': {name: data[name].tolist() for name in data.keys()} if isinstance(data, dict) else data.tolist()}

    def __score_model(self, dataset):
        url = self.url
        headers = {'Authorization': f'Bearer {self.token}', 'Content-Type': 'application/json'}
        ds_dict = {'dataframe_split': dataset.to_dict(orient='split')} if isinstance(dataset, pd.DataFrame) else self.__create_tf_serving_json(dataset)
        data_json = json.dumps(ds_dict, allow_nan=True)
        response = requests.request(method='POST', headers=headers, url=url, data=data_json)
        if response.status_code != 200:
            raise Exception(f'Request failed with status {response.status_code}, {response.text}')
        return response.json()
    
    def main(self, dataset_para_enviar):
        try:
            resultado = self.__score_model(dataset_para_enviar)
            return resultado
        except Exception as e:
            print(f"Error en la comunicación: {e}")