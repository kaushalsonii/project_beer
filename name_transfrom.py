from pathlib import Path
import json
import csv
from datetime import datetime


class Clean:   
    def __init__(self):
        self.new_data = []

    def name_data(self, page):
        with Path("data/raw_data/{}.json".format(page)).open(encoding="UTF-8") as source:
            json_data = json.load(source)

            for element in json_data:
                id = element["id"]
                name = element["name"]
                try:
                    date = datetime.strptime(element["first_brewed"], "%Y")
                except ValueError:
                    date = datetime.strptime(element["first_brewed"], "%m/%Y")
                date_formatted = date.strftime("%Y-%m-%d")
                metrics = ["abv", "ibu", "srm", "ph", "target_fg"]
                for metric in metrics:
                    if metric in element:
                        metric_name = metric
                        metric_value = element[metric]
                        yeast_type = "Unknown"
                        if element['ingredients']['yeast'] is not None:
                            if 'Ale' in element['ingredients']['yeast'] or 'ale' in element['ingredients']['yeast']:
                                yeast_type = 'Ale'
                            elif 'Lager' in element['ingredients']['yeast'] or 'lager' in element['ingredients']['yeast']:
                                yeast_type = 'Lager'
                            else:
                                yeast_type = "Other"
                            self.new_data.append([id, name, date_formatted, metric_name, metric_value, yeast_type])
            
            
    def make_csv(self):    
        with open("data/transform_data/name_table.csv", "w", newline="", encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name", "date", "metric_name", "metric_value", "yeast_type"])
            writer.writerows(self.new_data)

demo = Clean()
for x in range(1,6):
    demo.name_data(page=x)
demo.make_csv()
