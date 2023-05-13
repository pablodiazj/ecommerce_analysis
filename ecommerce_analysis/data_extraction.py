"""docstring"""
import os
import logging
from urllib import request
import json

class DataExtraction:
    """docstring"""
    def extract_from_url(self, url_to_read, site=None, query=None):
        """docstring"""
        if query is not None:
            query_str = f'/{site}/search?q={query}'
            url_to_read = url_to_read + query_str
        with request.urlopen(url_to_read) as url:
            data = json.load(url)
            logging.info(f'datos extraidos desde servicio')

        return data, query

    def store_as_json(self, data, folder_to_store, filename):
        """docstring"""
        data_dumped = json.dumps(data)

        try:
            os.makedirs(folder_to_store)
        except FileExistsError:
            logging.info(f'carpeta {folder_to_store} ya existe')

        with open(folder_to_store +f'/{filename}.json' # +f'\\{filename}.json'
                  , encoding='utf-8', mode='w') as file:
            json.dump(data_dumped, file, indent=4)
            logging.info(f'datos escritos en json')


if __name__=='__main__':
    iterate_over = {'url_to_read': ["https://api.mercadolibre.com/sites", "https://api.mercadolibre.com/sites"],
                    'folder_to_store': ["./data/stage=raw/source=sites/dataformat=json", "./data/stage=raw/source=search/dataformat=json"],
                    'filename': ["sites", "{site}_{query}"],
                    'site': [None, "MLA"],
                    'query': [None, "tv%204k"]}

    data_extractor = DataExtraction()

    for i in range(len(iterate_over['url_to_read'])):
        site = iterate_over['site'][i]
        query = iterate_over['query'][i]

        data, query = data_extractor.extract_from_url(url_to_read=iterate_over['url_to_read'][i],
                                                      site=site,
                                                      query=query)
        data_extractor.store_as_json(data,
                                    folder_to_store=iterate_over['folder_to_store'][i], 
                                    filename=iterate_over['filename'][i].format(site=site, query=query))
