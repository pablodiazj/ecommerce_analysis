import pandas as pd

from sklearn.model_selection import cross_val_score, StratifiedKFold, train_test_split
from sklearn.ensemble import RandomForestRegressor

import xgboost as xgb
import lightgbm as lgbm
import joblib
import os
import logging

class Preprocess():

    def drop_duplicated(self, pdf, cols):
        return pdf.drop_duplicates(subset=cols)
    
    def calc_discount_percentage(self, pdf):
        pdf['discount_percentage'] = pdf\
            .apply(lambda x: x['price']/x['original_price'] - 1 if (x['has_discount']==1) and (~pd.isna(x['price'])) else 0, axis=1)
        return pdf
    
    def calc_has_discount(self, pdf):
        pdf['has_discount'] = pdf['original_price']\
            .apply(lambda x: 0 if pd.isna(x) else 1)
        return pdf
    
    def get_seller_data(self, pdf):
        return None

# class Models():

class Utils():
    def get_file_folder(self, path):
        return '/'.join(path.split('/')[:-1])

    def save_joblib(self, obj, path):
        folder_path = self.get_file_folder(path)
        try:
            os.makedirs(folder_path)
        except FileExistsError:
            logging.warning(f'path {folder_path} already exists')
            pass

        joblib.dump(obj, path)
        logging.info(f'data stored in {path}')

    def save_parquet(self, obj, path):
        folder_path = self.get_file_folder(path)
        try:
            os.makedirs(folder_path)
        except FileExistsError:
            logging.warning(f'path {folder_path} already exists')
            pass

        # joblib.dump(obj, path)
        obj.to_parquet(path, engine='pyarrow')
        logging.info(f'data stored in {path}')
