import preprocess
import pandas as pd
import lightgbm

import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats import t
from scipy.stats import ttest_ind

search_folder = "/workspaces/ecommerce_analysis/data/stage=raw/source=search/dataformat=parquet"
sites_folder = "/workspaces/ecommerce_analysis/data/stage=raw/source=sites/dataformat=parquet"
publications_folder = "/workspaces/ecommerce_analysis/data/stage=raw/source=search_publications/dataformat=parquet"

money_conversion = {'ARS': 0.0043,
                    'BRL': 0.2,
                    'CLP': 0.0013,
                    'PEN': 0.27}

categorical_features = ['listing_type_id', 'buying_mode', 'site_id', 'category_id', 'has_discount', 'accepts_mercadopago', 'condition']
numerical_features = ['order_backend', 'price', 'discount_percentage', 'available_quantity']
y = ['sold_quantity']

# ## diccionarios:
# - tags (otras descripciones - jerarquias a investigar)
# - seller (otras jerarquias a investigar)
# - seller_address (direccion para jerarquia paises, ej: ciudad)
# - address (idem anterior)
# - attributes (investigar)
# - installments (investigar)

search_results = pd.read_parquet(search_folder, engine="pyarrow")

preprocesor = preprocess.Preprocess()

train_df = preprocesor.drop_duplicated(pdf=search_results, cols=['id'])
train_df = preprocesor.calc_has_discount(pdf=train_df)
train_df = preprocesor.calc_discount_percentage(pdf=train_df)
print(train_df.columns)

import numpy as np
import pandas as pd

from hyperopt import hp, tpe
from hyperopt.fmin import fmin

from sklearn.model_selection import cross_val_score, StratifiedKFold
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import make_scorer

import xgboost as xgb

import lightgbm as lgbm

X = train_df[numerical_features]
X = pd.concat([X, pd.get_dummies(train_df[categorical_features], columns=categorical_features)], ignore_index=False)
Y = train_df[y[0]]

def objective(params):
    params = {'n_estimators': int(params['n_estimators']), 'max_depth': int(params['max_depth'])}
    clf = RandomForestRegressor(n_jobs=4, **params)
    score = cross_val_score(clf, X, Y, cv=StratifiedKFold(), scoring='neg_root_mean_squared_error').mean()
    print("Score {:.2f} params {}".format(score, params))
    return score

space = {
    'n_estimators': hp.quniform('n_estimators', 25, 500, 25),
    'max_depth': hp.quniform('max_depth', 1, 10, 1)
}

best_rf = fmin(fn=objective,
            space=space,
            algo=tpe.suggest,
            max_evals=10)

for key, value in best_rf.items():
    best_rf[key] = int(value)
print(best_rf)

rf_model = RandomForestRegressor(
    n_jobs=4,
    **best_rf)

def objective(params):
    params = {
        'max_depth': int(params['max_depth']),
        'gamma': "{:.3f}".format(params['gamma']),
        'colsample_bytree': '{:.3f}'.format(params['colsample_bytree']),
    }
    
    clf = xgb.XGBRegressor(
        n_estimators=250,
        learning_rate=0.05,
        n_jobs=4,
        **params
    )
    
    score = cross_val_score(clf, X, Y, cv=StratifiedKFold(), scoring='neg_root_mean_squared_error').mean()
    print("Score {:.3f} params {}".format(score, params))
    return score

space = {
    'max_depth': hp.quniform('max_depth', 2, 8, 1),
    'colsample_bytree': hp.uniform('colsample_bytree', 0.3, 1.0),
    'gamma': hp.uniform('gamma', 0.0, 0.5),
}

best_xgb = fmin(fn=objective,
            space=space,
            algo=tpe.suggest,
            max_evals=10)

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

def objective(params):
    params = {
        'num_leaves': int(params['num_leaves']),
        'colsample_bytree': '{:.3f}'.format(params['colsample_bytree']),
    }
    
    clf = lgbm.LGBMRegressor(
        n_estimators=500,
        learning_rate=0.01,
        **params
    )
    
    score = cross_val_score(clf, X, Y, cv=StratifiedKFold(), scoring='neg_root_mean_squared_error').mean()
    print("Score {:.3f} params {}".format(score, params))
    return score

space = {
    'num_leaves': hp.quniform('num_leaves', 8, 128, 2),
    'colsample_bytree': hp.uniform('colsample_bytree', 0.3, 1.0),
}

best_lgbm = fmin(fn=objective,
            space=space,
            algo=tpe.suggest,
            max_evals=10)

print(best_lgbm)

best_lgbm['num_leaves'] = int(best_lgbm['num_leaves'])
best_lgbm['colsample_bytree'] = float(best_lgbm['colsample_bytree'])

lgbm_model = lgbm.LGBMRegressor(
        n_estimators=500,
        learning_rate=0.01,
        **best_lgbm
    )

print(cross_val_score(rf_model, X, Y, cv=StratifiedKFold(), scoring='neg_root_mean_squared_error').mean())
print(cross_val_score(xgb_model, X, Y, cv=StratifiedKFold(), scoring='neg_root_mean_squared_error').mean())
print(cross_val_score(lgbm_model, X, Y, cv=StratifiedKFold(), scoring='neg_root_mean_squared_error').mean())
