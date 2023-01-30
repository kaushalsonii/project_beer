from google.cloud import bigquery
from google.oauth2.service_account import Credentials

credentials = Credentials.from_service_account_file("")
client = bigquery.Client(credentials=credentials)

class Load:
    def create_name_table(self,table_id,file_path): 
        schema =[
                    bigquery.SchemaField("id", "INTEGER"),
                    bigquery.SchemaField("name", "STRING"),
                    bigquery.SchemaField("date", "DATE"),
                    bigquery.SchemaField("metric_name", "STRING"),
                    bigquery.SchemaField("metric_value", "FLOAT"),
                    bigquery.SchemaField("yeast", "STRING")] 

        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)  
        job_config = bigquery.LoadJobConfig(
            source_format = bigquery.SourceFormat.CSV, skip_leading_rows = 1,
            write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE)

        with open (file_path, 'rb') as source_file:
                job = client.load_table_from_file(source_file, table_id, job_config=job_config)
                print('done_NAME')


    def create_ingredients_table(self,table_id,file_path):
        schema =[   bigquery.SchemaField("name", "STRING"),
                    bigquery.SchemaField("amount", "FLOAT"),
                    bigquery.SchemaField("id", "INTEGER"),
                    bigquery.SchemaField("type", "STRING"),
                    bigquery.SchemaField("add", "STRING"),
                    bigquery.SchemaField("attribute", "STRING")]
                    

        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)  
        job_config = bigquery.LoadJobConfig(
            source_format = bigquery.SourceFormat.CSV, skip_leading_rows = 1,
            write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
)

        with open (file_path, 'rb') as source_file:
                job = client.load_table_from_file(source_file, table_id, job_config=job_config)
                print('done_TABLE')

        


name_table = Load()
name_table.create_name_table(table_id = "",
file_path = "C:/Users/Admin/Desktop/wk_space/data/transform_data/name_table.csv")

ingredients_table = Load()
ingredients_table.create_ingredients_table(table_id = "",
file_path = "C:/Users/Admin/Desktop/wk_space/data/transform_data/ingredients_table.csv")
