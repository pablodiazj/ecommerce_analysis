import preprocess
import pandas as pd

from hyperopt import hp, tpe
from hyperopt.fmin import fmin

from sklearn.model_selection import cross_val_score, StratifiedKFold, train_test_split
from sklearn.ensemble import RandomForestRegressor

import xgboost as xgb
import lightgbm as lgbm

import joblib
import os
import logging

search_folder = "/workspaces/ecommerce_analysis/data/stage=raw/source=search/dataformat=parquet"
sites_folder = "/workspaces/ecommerce_analysis/data/stage=raw/source=sites/dataformat=parquet"
publications_folder = "/workspaces/ecommerce_analysis/data/stage=raw/source=search_publications/dataformat=parquet"

money_conversion = {'ARS': 0.0043,
                    'BRL': 0.2,
                    'CLP': 0.0013,
                    'PEN': 0.27}

scoring = 'neg_mean_absolute_error'
max_evals = 30

categorical_features = ['listing_type_id', 'buying_mode', 'site_id', 'category_id', 'has_discount', 'accepts_mercadopago', 'condition']
numerical_features = ['order_backend', 'price', 'discount_percentage', 'available_quantity']
y = ['sold_quantity']

path_to_store_models = "/workspaces/ecommerce_analysis/models/"
path_to_store_datasets = "/workspaces/ecommerce_analysis/data/stage=preprocess/"

space_rf = {
    'n_estimators': hp.quniform('n_estimators', 25, 500, 25),
    'max_depth': hp.quniform('max_depth', 1, 20, 1)
}

space_xgb = {
    'max_depth': hp.quniform('max_depth', 2, 20, 1),
    'colsample_bytree': hp.uniform('colsample_bytree', 0.3, 1.0),
    'gamma': hp.uniform('gamma', 0.0, 0.5),
}

space_lgbm = {
    'num_leaves': hp.quniform('num_leaves', 8, 256, 2),
    'colsample_bytree': hp.uniform('colsample_bytree', 0.3, 1.0),
}

# ## diccionarios:
# - tags (otras descripciones - jerarquias a investigar)
# - seller (otras jerarquias a investigar)
# - seller_address (direccion para jerarquia paises, ej: ciudad)
# - address (idem anterior)
# - attributes (investigar)
# - installments (investigar)

search_results = pd.read_parquet(search_folder, engine="pyarrow")

# Preprocess
preprocesor = preprocess.Preprocess()

train_df = preprocesor.drop_duplicated(pdf=search_results, cols=['id'])
train_df = preprocesor.calc_has_discount(pdf=train_df)
train_df = preprocesor.calc_discount_percentage(pdf=train_df)
print(train_df.columns)

# Training
X = train_df[numerical_features]
X = pd.concat([X, pd.get_dummies(train_df[categorical_features], columns=categorical_features)], axis=1)
Y = train_df[y[0]]

# Train / test split
X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.3, random_state=187)

utilidades = preprocess.Utils()
utilidades.save_joblib({'X_train': X_train, 'X_test': X_test,
             'Y_train': Y_train, 'Y_test': Y_test}, path_to_store_datasets + 'train_test_split.joblib')

def objective_rf(params):
    params = {'n_estimators': int(params['n_estimators']), 'max_depth': int(params['max_depth'])}
    clf = RandomForestRegressor(n_jobs=4, criterion='squared_error', **params)
    score = cross_val_score(clf, X_train, Y_train, cv=StratifiedKFold(), scoring=scoring).mean()
    print("Score {:.2f} params {}".format(score, params))
    return score

def objective_xgb(params):
    params = {
        'max_depth': int(params['max_depth']),
        'gamma': "{:.3f}".format(params['gamma']),
        'colsample_bytree': '{:.3f}'.format(params['colsample_bytree']),
    }
    
    clf = xgb.XGBRegressor(
        n_estimators=250,
        learning_rate=0.001,
        n_jobs=4,
        **params
    )
    
    score = cross_val_score(clf, X_train, Y_train, cv=StratifiedKFold(), scoring=scoring).mean()
    print("Score {:.3f} params {}".format(score, params))
    return score

def objective_lgbm(params):
    params = {
        'num_leaves': int(params['num_leaves']),
        'colsample_bytree': '{:.3f}'.format(params['colsample_bytree']),
    }
    
    clf = lgbm.LGBMRegressor(
        n_estimators=500,
        learning_rate=0.001,
        **params
    )
    
    score = cross_val_score(clf, X_train, Y_train, cv=StratifiedKFold(), scoring=scoring).mean()
    print("Score {:.3f} params {}".format(score, params))
    return score

# modelos = preprocess.Models()
best_rf = fmin(fn=objective_rf,
            space=space_rf,
            algo=tpe.suggest,
            max_evals=max_evals)

for key, value in best_rf.items():
    best_rf[key] = int(value)
print(best_rf)

rf_model = RandomForestRegressor(
    n_jobs=4,
    **best_rf)

best_xgb = fmin(fn=objective_xgb,
            space=space_xgb,
            algo=tpe.suggest,
            max_evals=max_evals)

print(best_xgb)

best_xgb['max_depth'] = int(best_xgb['max_depth'])
best_xgb['colsample_bytree'] = float(best_xgb['colsample_bytree'])
best_xgb['gamma'] = float(best_xgb['gamma'])

xgb_model = xgb.XGBRegressor(
        n_estimators=250,
        learning_rate=0.05,
        n_jobs=4,
        **best_xgb
    )

best_lgbm = fmin(fn=objective_lgbm,
            space=space_lgbm,
            algo=tpe.suggest,
            max_evals=max_evals)

print(best_lgbm)

best_lgbm['num_leaves'] = int(best_lgbm['num_leaves'])
best_lgbm['colsample_bytree'] = float(best_lgbm['colsample_bytree'])

lgbm_model = lgbm.LGBMRegressor(
        n_estimators=500,
        learning_rate=0.01,
        **best_lgbm
    )

print(cross_val_score(rf_model, X, Y, cv=StratifiedKFold(), scoring=scoring).mean())
print(cross_val_score(xgb_model, X, Y, cv=StratifiedKFold(), scoring=scoring).mean())
print(cross_val_score(lgbm_model, X, Y, cv=StratifiedKFold(), scoring=scoring).mean())

try:
    os.makedirs(path_to_store_models)
except FileExistsError:
    logging.warning(f'path {path_to_store_models} already exists')
    pass

joblib.dump(rf_model, path_to_store_models + 'rf_model.joblib')
joblib.dump(xgb_model, path_to_store_models + 'xgb_model.joblib')
joblib.dump(lgbm_model, path_to_store_models + 'lgbm_model.joblib')
