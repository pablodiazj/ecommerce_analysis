import pandas as pd

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
