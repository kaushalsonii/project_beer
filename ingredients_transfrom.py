import pandas as pd
import json
from pathlib import Path

class Clean:
    def __init__(self):
        self.ing_temp = []

    def ingredients_table(self, page):
        with Path(f"data/raw_data/{page}.json").open(encoding="UTF-8") as source:
            json_data = json.load(source)
            for ing in range(len(json_data)):        
                malt_df = pd.DataFrame(json_data[ing]['ingredients']['malt'])
                hop_df = pd.DataFrame(json_data[ing]['ingredients']['hops'])
                malt_df['id'] = json_data[ing]['id']
                if malt_df.empty:
                    malt_df['amount'] = 0
                else:
                    malt_df['amount'] = malt_df['amount'].apply(lambda item: item.get('value', 0) * 1000)

                hop_df['id'] = json_data[ing]['id']
                if hop_df.empty:
                    hop_df['amount'] = 0
                else:    
                    hop_df['amount'] = hop_df['amount'].apply(lambda item: item.get('value', 0))
                malt_df['type'] = 'malt'
                hop_df['type'] = 'hop'
                ingredients = pd.concat([malt_df, hop_df])
                self.ing_temp.append(ingredients)
    def make_csv(self):    
        ing_data = pd.concat(self.ing_temp)
        ing_data.to_csv('data/transform_data/ingredients_table.csv', index=False)

                
demo = Clean()
for y in range(1, 6):
    demo.ingredients_table(page=y)
demo.make_csv()