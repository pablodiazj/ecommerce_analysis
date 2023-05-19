import joblib
from sklearn.model_selection import cross_val_score, StratifiedKFold

path_to_store_datasets = "/workspaces/ecommerce_analysis/data/stage=preprocess/"
path_to_store_models = "/workspaces/ecommerce_analysis/models/"
scoring = 'neg_mean_absolute_error'

dict_data = joblib.load(path_to_store_datasets + 'train_test_split.joblib')
X_train = dict_data['X_train']
X_test = dict_data['X_test']
Y_train = dict_data['Y_train']
Y_test = dict_data['Y_test']

rf_model = joblib.load(path_to_store_models + 'rf_model.joblib')
xgb_model = joblib.load(path_to_store_models + 'xgb_model.joblib')
lgbm_model = joblib.load(path_to_store_models + 'lgbm_model.joblib')

print(cross_val_score(rf_model, X_test, Y_test, cv=StratifiedKFold(), scoring=scoring).mean())
print(cross_val_score(xgb_model, X_test, Y_test, cv=StratifiedKFold(), scoring=scoring).mean())
print(cross_val_score(lgbm_model, X_test, Y_test, cv=StratifiedKFold(), scoring=scoring).mean())
