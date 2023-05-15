"""docstring"""
import os
import logging
import glob
import json
import pandas as pd

class DataStructure:
    """"""
    def transform_json_to_parquet(self, folder_to_read):
        """"""
        # archivos a transformar
        files_to_transform = glob.glob(folder_to_read + '/*') # '\*'

        pdf = pd.DataFrame()

        publications_sites_written = []

        for file in files_to_transform:
            with open(file,encoding='utf-8') as json_file:
                json_loaded = json.load(json_file)

            try:
                pdf = pdf.append(pd.DataFrame(json.loads(json_loaded)['results']))
                # pdf = pd.DataFrame(json.loads(json_loaded)['results'])
            except (KeyError, TypeError):
                pdf = pd.DataFrame(json.loads(json_loaded))

            file_to_write = file\
                    .replace('dataformat=json', 'dataformat=parquet')\
                    .replace('json', 'parquet')
            
            try:
                # folder_to_store = '\\'.join(file_to_write.split('\\')[:-1])
                folder_to_store = '/'.join(file_to_write.split('/')[:-1])
                os.makedirs(folder_to_store)
            except FileExistsError:
                logging.info(f'carpeta {folder_to_store} ya existe')
            
            if 'source=search' in file_to_write:
                publications_site = file_to_write.split('/')[-1]
                publications_site = publications_site.split('_')[0]
                if publications_site not in publications_sites_written:
                    publications_file = '/'.join(file_to_write.split('/')[:-1]) + f'/site={publications_site}/publications.parquet'
                    publications_file = publications_file.replace('source=search', 'source=search_publications')
                    publications = pd.DataFrame(json.loads(json_loaded)['paging'], index=[0])

                    try:
                        folder_to_store_pubs = '/'.join(publications_file.split('/')[:-1])
                        os.makedirs(folder_to_store_pubs)
                    except FileExistsError:
                        logging.info(f'carpeta {folder_to_store_pubs} ya existe')
                    
                    publications.to_parquet(path=publications_file, engine='pyarrow')
                    publications_sites_written.append(publications_site)
            
        pdf.to_parquet(path=file_to_write, engine='pyarrow')
        logging.info(f'datos escritos en parquet')

if __name__=='__main__':
    iterate_over = {'folder_to_read': ["./data/stage=raw/source=sites/dataformat=json", "./data/stage=raw/source=search/dataformat=json"]}
    
    data_structure = DataStructure()
    
    for i in range(len(iterate_over['folder_to_read'])):
        data_structure.transform_json_to_parquet(folder_to_read=iterate_over['folder_to_read'][i]
                                    # 'D:\Study\mercado_libre\ecommerce_analysis\data\stage=raw\source=search\dataformat=json'
                                    )
        
