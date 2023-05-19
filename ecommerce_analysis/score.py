#!/usr/bin/env python
# encoding: utf-8
# código para ejecutar inferencia
import joblib
import json
from flask import Flask, jsonify, request
import pandas as pd
import logging

class Score():
    '''clase utilizada para predecir en batch o dejar api disponible'''
    def calculate_score_df(self, pdf, model):
        '''calcular predicciones de pandas dataframe'''
        return model.predict(pdf)

    def get_score_batch(self):
        '''calcular predicciones batch'''
        # rutas lectura datasets
        path_to_store_datasets = "/workspaces/ecommerce_analysis/data/stage=preprocess/"

        dict_data = joblib.load(path_to_store_datasets + 'train_test_split.joblib')

        X_test = dict_data['X_test']

        scorer = Score()
        X_test['pred'] = scorer.calculate_score_df(X_test, model)
        logging.info('predictions stored')

# rutas lectura modelos
path_to_store_models = "/workspaces/ecommerce_analysis/models/"

# api
app = Flask(__name__)

# best model
model = joblib.load(path_to_store_models + 'best_model.joblib')

@app.route('/', methods=['POST'])
def get_score_api():
    '''metodo post para obtener prediccion'''
    record = json.loads(request.data)
    record['predict'] = model.predict(pd.DataFrame([record]))[0]
    return jsonify(record)

app.run()
