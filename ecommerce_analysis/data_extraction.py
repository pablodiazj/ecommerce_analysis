"""docstring"""
import os
import logging
import urllib.request, json

class DataExtraction:
    """"""
    def __init__(self, url_to_read, folder_to_store):
        self.url_to_read = url_to_read
        self.folder_to_store = folder_to_store

    def extract_by_query(self, query):
        with urllib.request.urlopen(self.url_to_read.format(query=query)) as url:
            data = json.load(url)
            logging.info(f'datos extraidos desde servicio')

        return data, query
    
    def store_as_json(self, data, filename):
        data_dumped = json.dumps(data)

        try:
            os.makedirs(self.folder_to_store)
        except FileExistsError:
            logging.info(f'carpeta {self.folder_to_store} ya existe')

        with open(self.folder_to_store+f'\\{filename}.json', encoding='utf-8', mode='w') as file:
            json.dump(data_dumped, file, indent=4)
            logging.info(f'datos escritos en json')


if __name__=='__main__':
    data_extractor = DataExtraction(url_to_read="https://api.mercadolibre.com/sites/MLA/search?q={query}", 
                                    folder_to_store='D:\Study\mercado_libre\ecommerce_analysis\data\stage=raw\source=search\dataformat=json')
    data, query = data_extractor.extract_by_query(query="tv%204k")

    data_extractor.store_as_json(data=data, filename=query)
