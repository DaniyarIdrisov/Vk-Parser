import requests
import time
import psycopg2

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import re

args = {
    'owner': 'daniyar',
    'start_date': datetime.datetime(2021, 4, 7),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'depends_on_past': False,
}

access_token = 'eada3759eada3759eada3759e6eaacaccdeeadaeada37598aedeb349664d3a96659dc6f'
domain_name = 'itis_kfu'
vk_api_version = '5.130'
number_of_posts = 100
offset_size = 0
connection = psycopg2.connect(database='postgres', user='postgres', password='qwerty007', host='datamining-db.c9tzcqwjjfnn.us-east-1.rds.amazonaws.com', port='5432')


def get_posts():
    posts_array = []

    offset_size = 0

    while offset_size <= 100:
        response = requests.get('https://api.vk.com/method/wall.get',
                                params={
                                    'access_token': access_token,
                                    'v': vk_api_version,
                                    'domain': domain_name,
                                    'count': number_of_posts,
                                    'offset': offset_size
                                }
                                )
        current_posts = response.json()['response']['items']
        offset_size = offset_size + 100
        posts_array.extend(current_posts)
        time.sleep(0.6)
    return posts_array


def create_unique_words_map(posts):
    punctuation = ['(', ')', '?', ':', ';', ',', '.', '!', '/', '"', "'"]
    map = {}

    for i in posts:
        text = i['text']
        for i in text:
            if i in punctuation:
                text.replace(i, "")
        words_array = text.split()
        for k in words_array:
            l = words_array.index(k)
            if k.startswith('#') or k.startswith('http') or k.startswith('/') or k.startswith('[') or k.startswith(
                    ']') or k.startswith('{') or k.startswith('}'):
                words_array[l] = ''
        for i in words_array:
            if i in map.keys():
                map[i] = map[i] + 1
            else:
                map[i] = 1
    map.pop('-')
    map.pop('')
    map = sorted(map.items(), key=lambda para: (para[1], para[0]), reverse=True)

    return map


def write_into_database():
    posts = get_posts()
    map = create_unique_words_map(posts)
    cursor = connection.cursor()
    for key in map:
        cursor.execute("INSERT INTO words(word, count) VALUES ('" + key[0] + "', " + str(key[1]) + ")")
        connection.commit()


with DAG(dag_id='parser_dag', default_args=args, schedule_interval=None) as dag:
    vk_parser = PythonOperator(
        task_id='parser_words',
        python_callable=write_into_database,
        dag=dag
    )