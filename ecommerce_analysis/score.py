# código para ejecutar inferencia
import joblib

# Agregar features
class Score():
    def calculate_score_df(self, pdf, model):
        return model.predict(pdf)

path_to_store_models = "/workspaces/ecommerce_analysis/models/"
path_to_store_datasets = "/workspaces/ecommerce_analysis/data/stage=preprocess/"

# best model
model = joblib.load(path_to_store_models + 'best_model.joblib')
dict_data = joblib.load(path_to_store_datasets + 'train_test_split.joblib')

X_train = dict_data['X_train']
X_test = dict_data['X_test']
Y_train = dict_data['Y_train']
Y_test = dict_data['Y_test']

scorer = Score()
X_test['pred'] = scorer.calculate_score_df(X_test, model)

#!/usr/bin/env python
# encoding: utf-8
import json
from flask import Flask, jsonify, request

app = Flask(__name__)

@app.route('/', methods=['POST'])
def score_record():
    record = json.loads(request.data)
    record['paso'] = 'si'
    return jsonify(record)

app.run()
