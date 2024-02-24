# import os, json, boto3, pathlib, psycopg2, requests
# import pandas as pd
# from airflow import DAG
# from pathlib import Path
# import airflow.utils.dates
# from airflow.sensors.python import PythonSensor
# from airflow.operators.python import PythonOperator
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# import csv , json

# CUR_DIR = os.path.abspath(os.path.dirname(__file__))


# dag1 = DAG(
#     dag_id="get_university_api_send_psql",
#     start_date=airflow.utils.dates.days_ago(1),
#     schedule_interval=None
#     )

# dag2 = DAG(
#     dag_id="get_psql_data_send_s3",
#     start_date=airflow.utils.dates.days_ago(1),
#     schedule_interval=None)


# def _fetch_and_save():
#     # Fetch data from API
#     response = requests.get('http://universities.hipolabs.com/search?country=')
#     data_json = response.json()

#     # Check if path is exists or create
#     main_path = "/tmp/json_file"
#     pathlib.Path(main_path).mkdir(parents=True, exist_ok=True)

#     # Prepare CSV file path
#     csv_file = main_path + "/data_university.csv"

#     # Open CSV file in write mode 
#     with open(csv_file, 'w', newline='') as file:
#         writer = csv.writer(file)
        
#         # Write header
#         writer.writerow(['country', 'alpha_two_code', 'name', 'state-province', 'domains', 'web_pages'])
        
#         # Iterate over each JSON object and extract required fields
#         for item in data_json:
#             country = item.get('country', '')
#             alpha_two_code = item.get('alpha_two_code', '')
#             name = item.get('name', '')
#             state_province = item.get('state-province', '')
#             domains = ', '.join(item.get('domains', []))
#             web_pages = ', '.join(item.get('web_pages', []))
            
#             # Write row to CSV
#             writer.writerow([country, alpha_two_code, name, state_province, domains, web_pages])

#     # Print success message
#     print('Data has been loaded into CSV:', csv_file)




# def _read_json_file():
#     main_path = "/tmp/json_file"

#     csv_file_path = main_path + "/data_university.csv"
#     json_file_path = main_path +  "/data_university.json"
#     # Open the CSV file for reading
#     with open(csv_file_path, 'r') as csv_file:
#         # Create a CSV reader object
#         csv_reader = csv.DictReader(csv_file)

#         # Initialize an empty list to store the rows
#         rows = []

#         # Iterate over each row in the CSV file
#         for row in csv_reader:
#             # Create a dictionary with the desired fields
#             data = {
#                 'country': row['country'],
#                 'alpha_two_code': row['alpha_two_code'],
#                 'name': row['name'],
#                 'state-province': row['state-province'],
#                 'domains': row['domains'],
#                 'web_pages': row['web_pages']
#             }

#             # Append the dictionary to the list
#             rows.append(data)

#     # Open the JSON file for writing
#     with open(json_file_path, 'w') as json_file:
#         # Write the list of dictionaries to the JSON file
#         json.dump(rows, json_file)



# def _send_to_postgresql():

#     json_file_path = '/tmp/json_file/data_university.json'

#     # Connect to the PostgreSQL database
#     conn = psycopg2.connect(
#         database="airflow", 
#         user="airflow", 
#         password="airflow", 
#         host="host.docker.internal",
#         port="5432"
#     )

#     # Create a cursor object to interact with the database
#     cursor = conn.cursor()


#     cursor.execute("""CREATE TABLE IF NOT EXISTS university_table(
#                         id SERIAL PRIMARY KEY NOT NULL, 
#                         country VARCHAR(255), 
#                         alpha_two_code VARCHAR(2),
#                         name VARCHAR(255), 
#                         state_province VARCHAR(255),
#                         domains TEXT[],
#                         web_pages TEXT[]
#                     )""")

#     # Read the JSON data from the file
#     with open(json_file_path, 'r') as json_file:
#         json_data = json.load(json_file)

#     # Insert each JSON record into the database table
#     for record in json_data:
#         country = record['country']
#         alpha_two_code = record['alpha_two_code']
#         name = record['name']
#         state_province = record['state-province']
#         domains = record['domains']
#         web_pages = record['web_pages']

#         # Execute the INSERT query
#         cursor.execute("""
#             INSERT INTO university_table (country, alpha_two_code, name, state_province, domains, web_pages)
#             VALUES (%s, %s, %s, %s, ARRAY[%s], ARRAY[%s])
#         """, (country, alpha_two_code, name, state_province, domains, web_pages))

#     # Commit the changes to the database
#     conn.commit()

#     # Close the cursor and database connection
#     cursor.close()
#     conn.close()
#     os.remove("/tmp/json_file/data_university.json")






# def _wait_for_json(filepath):
#     return Path(filepath).exists()


# def _wait_for_csv(filepath):
#     return Path(filepath).exists()



# def _send_csv_s3():
#     with open(CUR_DIR + "/configurations.json","r") as output:
#         configurations = json.load(output)

#     s3_client = boto3.client(
#         service_name=configurations["service_name"],
#         region_name=configurations["region_name"],
#         aws_access_key_id=configurations["aws_access_key_id"],
#         aws_secret_access_key=configurations["aws_secret_access_key"])

#     s3_client.upload_file(
#         "/tmp/csv/data_university.csv",
#         configurations["bucket_name"],
#         configurations["file_name"])

#     os.remove("/tmp/csv/data_university.csv")



# def _fetch_psql_save_csv(file_path, csv_path):
#     conn = psycopg2.connect(
#         database="postgres_db",
#         user="postgres",
#         password="postgres",
#         host="host.docker.internal",
#         port="5433"
#     )

#     cursor = conn.cursor()

#     cursor.execute("""SELECT country, alpha_two_code, name, state_province, domains, web_pages 
#                         FROM university_table""")
#     all_data = cursor.fetchall()

#     countries = []
#     alpha_two_codes = []
#     names = []
#     state_provinces = []
#     domains = []
#     web_pages = []

#     for data in all_data:
#         countries.append(data[0])
#         alpha_two_codes.append(data[1])
#         names.append(data[2])
#         state_provinces.append(data[3])
#         domains.append(data[4])
#         web_pages.append(data[5])

#     dataframe = pd.DataFrame({
#         "country": countries,
#         "alpha_two_code": alpha_two_codes,
#         "name": names,
#         "state_province": state_provinces,
#         "domains": domains,
#         "web_pages": web_pages
#     })

#     pathlib.Path(file_path).mkdir(parents=True, exist_ok=True)

#     dataframe.to_csv(csv_path, index=False)
#     print("Dataframe saved!")

#     cursor.close()
#     conn.close()





# fetch_and_save = PythonOperator(
#     task_id="fetch_and_save",
#     python_callable=_fetch_and_save,
#     dag=dag1)

# # Poke interval is for every 30 seconds check
# # timeout is to prevent sensor deadlock
# # mode is reschedule to make free of the sensor's slot if it is not poking.
# wait_for_json = PythonSensor(
#     task_id="wait_for_json",
#     python_callable=_wait_for_json,
#     poke_interval=30,
#     timeout=24*60*60,
#     mode="reschedule",
#     op_kwargs={
#         "filepath":"/tmp/json_file/data_university.json"},
#     dag=dag1)

# read_json_file = PythonOperator(
#     task_id="read_json_file",
#     python_callable=_read_json_file,
#     dag=dag1)

# send_to_postgresql = PythonOperator(
#     task_id="send_to_postgresql",
#     python_callable=_send_to_postgresql,
#     dag=dag1)

# trigger_dag2 = TriggerDagRunOperator(
#     task_id="trigger_dag2", 
#     trigger_dag_id="get_psql_send_s3",
#     dag=dag1)

# fetch_psql_save_csv = PythonOperator(
#     task_id="fetch_psql_save_csv",
#     python_callable=_fetch_psql_save_csv,
#     op_kwargs={"file_path":"/tmp/csv",
#                "csv_path":"/tmp/csv/data_university.csv"},
#     dag=dag2)


# wait_for_csv = PythonSensor(
#     task_id="wait_for_csv",
#     python_callable=_wait_for_csv,
#     poke_interval=30,
#     timeout=24*60*60,
#     mode="reschedule",
#     op_kwargs={"filepath":"/tmp/csv/data_university.csv"},
#     dag=dag2)

# send_csv_s3 = PythonOperator(
#     task_id="send_csv_s3",
#     python_callable=_send_csv_s3,
#     dag=dag2)

# fetch_and_save >> read_json_file >> wait_for_json >> send_to_postgresql >> trigger_dag2

# fetch_psql_save_csv >> wait_for_csv >> send_csv_s3





