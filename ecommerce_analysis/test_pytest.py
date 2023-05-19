import pandas as pd
from urllib import request
import json

# data tests
def test_unicity_search_parquet():
    df = pd.read_parquet("/workspaces/ecommerce_analysis/data/stage=raw/source=search/dataformat=parquet")
    id_col = 'id'
    assert len(df) == len(df[id_col].unique())

test_case_api_score = {
    "order_backend": 25,
    "price": 5690.4,
    "discount_percentage": 0.0,
    "available_quantity": 1,
    "listing_type_id_bronze": 0,
    "listing_type_id_free": 0,
    "listing_type_id_gold_pro": 1,
    "listing_type_id_gold_special": 0,
    "buying_mode_buy_it_now": 1,
    "site_id_MLA": 0,
    "site_id_MLB": 1,
    "site_id_MLC": 0,
    "site_id_MPE": 0,
    "category_id_MLA1002": 0,
    "category_id_MLB1002": 1,
    "category_id_MLC1002": 0,
    "category_id_MPE1002": 0,
    "has_discount_0": 1,
    "has_discount_1": 0,
    "accepts_mercadopago_True": 1,
    "condition_new": 1,
    "condition_not_specified": 0,
    "condition_used": 0
}

def test_api():
    req = request.Request('http://127.0.0.1:5000/', method="POST")
    req.add_header('Content-Type', 'application/json')
    data = test_case_api_score
    data = json.dumps(data)
    data = data.encode()
    r = request.urlopen(req, data=data)
    content = r.read()
    print(content)
