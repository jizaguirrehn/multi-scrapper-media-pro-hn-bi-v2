import json
import logging
import os
import time

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

# Instancia del logger vinculada al contexto del módulo
logger = logging.getLogger(__name__)


class get_data:
    def __init__(self):
        load_dotenv()
        self.url = os.getenv("URL_DATABRICKS")
        self.token = os.getenv("DATABRICKS_TOKEN")

        if not self.url or not self.token:
            logger.warning(
                "Las variables de entorno URL_DATABRICKS o DATABRICKS_TOKEN no están definidas correctamente."
            )

    def __create_tf_serving_json(self, data):
        if isinstance(data, dict):
            return {'inputs': {name: data[name].tolist() for name in data.keys()}}
        return data.tolist()

    def __score_model(self, dataset):
        url = self.url
        headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }

        # Preparación del payload
        if isinstance(dataset, pd.DataFrame):
            ds_dict = {'dataframe_split': dataset.to_dict(orient='split')}
            total_rows = len(dataset)
        else:
            ds_dict = self.__create_tf_serving_json(dataset)
            total_rows = len(dataset) if hasattr(dataset, '__len__') else "desconocido"

        data_json = json.dumps(ds_dict, allow_nan=True)

        logger.debug(
            "Enviando petición a Databricks Serving Endpoint",
            extra={"url": url, "filas_payload": total_rows}
        )

        start_time = time.time()
        response = requests.post(url, headers=headers, data=data_json, timeout=160)
        duration = round(time.time() - start_time, 3)

        if response.status_code != 200:
            logger.error(
                "Fallo en la respuesta del Serving Endpoint de Databricks",
                extra={
                    "status_code": response.status_code,
                    "response_text": response.text,
                    "duracion_segundos": duration,
                }
            )
            raise Exception(f'Request failed with status {response.status_code}, {response.text}')

        logger.info(
            "Respuesta recibida exitosamente de Databricks Model Serving",
            extra={
                "status_code": response.status_code,
                "duracion_segundos": duration,
                "filas_procesadas": total_rows
            }
        )
        return response.json()

    def main(self, dataset_para_enviar):
        try:
            resultado = self.__score_model(dataset_para_enviar)
            return resultado
        except Exception:
            logger.exception("Error crítico durante la comunicación con Databricks Serving Endpoint")
            return None