import requests
import json

class PunkAPI:
    def get_data(self, page):
        response = requests.get(f'https://api.punkapi.com/v2/beers?page={page}&per_page=80')
        return response.json()


    def save_to_local(self,data,path):
        with open(path,"w") as f:
            json.dump(data,f,indent=4)

if __name__ == "__main__":
    api = PunkAPI()
    for i in range(1,6):
        data = api.get_data(page=i)
        api.save_to_local(data,f"data/raw_data/{i}.json")
