import data_extraction
import data_structuration
import model
import evaluation
import score
import os
import logging

build_api = True

if not os.path.isfile('/workspaces/ecommerce_analysis/data/stage=raw/source=sites/dataformat=parquet/sites.parquet'):
    logging.warning('descargando y construyendo datos iniciales')
    data_extraction.get()
    data_structuration.get()

model.get()
evaluation.get()
score.get_score_batch()

if build_api:
    score.build_score_api()
