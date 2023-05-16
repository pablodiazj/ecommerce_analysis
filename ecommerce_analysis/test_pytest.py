import pandas as pd

# data tests
def test_unicity_search_parquet():
    df = pd.read_parquet("/workspaces/ecommerce_analysis/data/stage=raw/source=search/dataformat=parquet")
    id_col = 'id'
    assert len(df) == len(df[id_col].unique())
