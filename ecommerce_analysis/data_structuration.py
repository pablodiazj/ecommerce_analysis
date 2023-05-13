"""docstring"""
import os
import logging
import glob
import json
import pandas as pd

class DataStructure:
    """"""
    def __init__(self, folder_to_read):
        self.folder_to_read = folder_to_read

    def transform_json_to_parquet(self):
        """"""
        # archivos a transformar
        files_to_transform = glob.glob(self.folder_to_read + '/*') # '\*'

        for file in files_to_transform:
            with open(file,encoding='utf-8') as json_file:
                json_loaded = json.load(json_file)

            pdf = pd.DataFrame(json.loads(json_loaded)['results'])

            file_to_write = file\
                    .replace('dataformat=json', 'dataformat=parquet')\
                    .replace('json', 'parquet')
            
            try:
                # folder_to_store = '\\'.join(file_to_write.split('\\')[:-1])
                folder_to_store = '/'.join(file_to_write.split('/')[:-1])
                os.makedirs(folder_to_store)
            except FileExistsError:
                logging.info(f'carpeta {folder_to_store} ya existe')
            
            pdf.to_parquet(path=file_to_write, engine='pyarrow')
            logging.info(f'datos escritos en parquet')

if __name__=='__main__':
    data_structure = DataStructure(folder_to_read='./data/stage=raw/source=search/dataformat=json'
                                   # 'D:\Study\mercado_libre\ecommerce_analysis\data\stage=raw\source=search\dataformat=json'
                                    )
    data_structure.transform_json_to_parquet()
