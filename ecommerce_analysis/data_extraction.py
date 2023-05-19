"""docstring"""
import os
import logging
from urllib import request
import json
import time

class DataExtraction:
    """clase contenedora de metodos para extraer datos"""
    def extract_from_url(self, url_to_read, site=None, query=None, offset=None):
        """descarga datos desde ciertas urls de mercado libre (api search)"""
        if query is not None:
            query_str = f'/{site}/search?q={query}'
            url_to_read = url_to_read + query_str
        
        if offset is not None:
            url_to_read = url_to_read + f'&offset={offset}'
        
        with request.urlopen(url_to_read) as url:
            data = json.load(url)
            logging.info(f'datos extraidos desde servicio')
            time.sleep(0.5)

        return data, query

    def store_as_json(self, data, folder_to_store, filename):
        """toma json obj, lo transforma a string y lo persiste en disco"""
        data_dumped = json.dumps(data)

        try:
            os.makedirs(folder_to_store)
        except FileExistsError:
            logging.info(f'carpeta {folder_to_store} ya existe')

        with open(folder_to_store +f'/{filename}.json' # +f'\\{filename}.json'
                  , encoding='utf-8', mode='w') as file:
            json.dump(data_dumped, file, indent=4)
            logging.info(f'datos escritos en json')


def get():
    '''metodo principal'''
    iterate_over = {'url_to_read': ["https://api.mercadolibre.com/sites", "https://api.mercadolibre.com/sites", "https://api.mercadolibre.com/sites", "https://api.mercadolibre.com/sites", "https://api.mercadolibre.com/sites"],
                    'folder_to_store': ["./data/stage=raw/source=sites/dataformat=json", "./data/stage=raw/source=search/dataformat=json", "./data/stage=raw/source=search/dataformat=json", "./data/stage=raw/source=search/dataformat=json", "./data/stage=raw/source=search/dataformat=json"],
                    'filename': ["sites", "{site}_{query}", "{site}_{query}", "{site}_{query}", "{site}_{query}"],
                    'site': [None, "MLA", "MLC", "MPE", "MLB"],
                    'query': [None, "tv%204k", "tv%204k", "tv%204k", "tv%204k"],
                    'offset': [None, 50, 50, 50, 50]}

    data_extractor = DataExtraction()

    for i in range(len(iterate_over['url_to_read'])):
        # TODO: resultados a consulta search trae solo los primeros 50 resultados, 
        # para obtener los siguientes 50, agregar offset=50 a la consulta
        site = iterate_over['site'][i]
        query = iterate_over['query'][i]
        offset = iterate_over['offset'][i]
        limit = 1000

        if offset is not None:
            for j in range(int(limit/offset) + 1):
                data, query = data_extractor.extract_from_url(url_to_read=iterate_over['url_to_read'][i],
                                                            site=site,
                                                            query=query,
                                                            offset=offset*j)
                data_extractor.store_as_json(data,
                                            folder_to_store=iterate_over['folder_to_store'][i],
                                            filename=iterate_over['filename'][i].format(site=site, query=f'{query}_{offset*j}'))
                print(f'Descargando pagina {j}')

        else:
            data, query = data_extractor.extract_from_url(url_to_read=iterate_over['url_to_read'][i],
                                                        site=site,
                                                        query=query)
            data_extractor.store_as_json(data,
                                        folder_to_store=iterate_over['folder_to_store'][i], 
                                        filename=iterate_over['filename'][i].format(site=site, query=query))
