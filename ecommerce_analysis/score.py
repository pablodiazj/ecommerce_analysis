#!/usr/bin/env python
# encoding: utf-8
# código para ejecutar inferencia
import joblib
import json
from flask import Flask, jsonify, request
import pandas as pd
import logging
import preprocess

class Score():
    '''clase utilizada para predecir en batch o dejar api disponible'''
    def calculate_score_df(self, pdf, model):
        '''calcular predicciones de pandas dataframe'''
        return model.predict(pdf)
    
    def get_basic_data(self):
        '''basic configs of scorer'''
        # rutas lectura datasets
        path_to_store_datasets = "/workspaces/ecommerce_analysis/data/stage=preprocess/"
        # rutas lectura modelos
        path_to_store_models = "/workspaces/ecommerce_analysis/models/"
        # best model
        model = joblib.load(path_to_store_models + 'best_model.joblib')

        return path_to_store_datasets, path_to_store_models, model

def get_score_batch():
    '''calcular predicciones batch'''
    scorer = Score()
    path_to_store_datasets, path_to_store_models, model = scorer.get_basic_data()

    dict_data = joblib.load(path_to_store_datasets + 'train_test_split.joblib')

    X_test = dict_data['X_test']

    X_test['pred'] = scorer.calculate_score_df(X_test, model)
    utilidades = preprocess.Utils()
    utilidades.save_parquet(X_test, path_to_store_datasets.replace('stage=preprocess', 'stage=results') + 'predictions.parquet')
    logging.info('predictions stored')

def build_score_api():
    '''metodo post para obtener prediccion'''
    # api
    app = Flask(__name__)
    
    scorer = Score()
    path_to_store_datasets, path_to_store_models, model = scorer.get_basic_data()

    @app.route('/', methods=['POST'])
    def get_score_api():
        '''metodo post para obtener prediccion'''
        record = json.loads(request.data)
        record['predict'] = model.predict(pd.DataFrame([record]))[0]
        return jsonify(record)

    app.run()
