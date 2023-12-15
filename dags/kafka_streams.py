from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Creating default arguments about the owner etc
default_args = {
    'owner': 'Anannya-M',
    'start_date': datetime(2023, 12, 15, 10, 25)
}

def get_data():
    import json
    import requests

    response = requests.get("https://randomuser.me/api/")
    response = response.json()
    response = response['results'][0]

    return response

def format_data(response):
    data = {}
    location = response['location']
    data['firstname'] = response['name']['first']
    data['lastname'] = response['name']['last']
    data['gender'] = response['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']} ,{location['state']}, {location['country']}"

    data['postcode'] = location['postcode']
    data['email'] = response['email']
    data['username'] = response['login']['username']
    data['dob'] = response['dob']['date']
    data['registered_date'] = response['registered']['date']
    data['phone'] = response['phone']
    data['picture'] = response['picture']['medium']

    return data

def stream_data():
    import json

    res = get_data()
    res = format_data(res)
    print(json.dumps(res, indent = 3))



# Creating a DAG
# with DAG('task_automation',
#          default_args = default_args,
#          schedule_interval = '@daily',
#          catchup = False) as dag:
#
#     streaming_task = PythonOperator(
#         task_id = 'stream_data_from_api',
#         python_callable = stream_data
#     )

stream_data()