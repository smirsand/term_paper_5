from typing import Any

import psycopg2
import requests


def db_create(database: str, **params):
    """ Создание базы данных."""

    conn = psycopg2.connect(**params)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(f'DROP DATABASE {database}')
    except:
        pass
    cur.execute(f'CREATE DATABASE {database}')
    cur.close()
    conn.close()


def get_company_vacancies(employer_id):
    """ Парсинг Head Hanter"""

    url = f'https://api.hh.ru/vacancies'
    params = {'employer_id': employer_id, 'per_page': 100, 'area': 2}
    response = requests.get(url, params=params)

    if response.status_code == 200:
        vacancies = []
        data = response.json()
        items = data.get('items', [])
        for vacancy in items:
            vacancies.append(vacancy)

        return vacancies

    else:
        print(f"Ошибка при запросе данных: {response.status_code}")


def insert_data_to_tables(vacancy: list[dict[str, Any]], database_name: str, params: dict[str, Any]) -> None:
    """ Функция для наполнения таблиц данными."""

    conn = psycopg2.connect(dbname=database_name, **params)
    with conn.cursor() as cur:
        list_id = []
        for row in vacancy:
            if row['employer']['id'] not in list_id:
                employer_id = row['employer']['id']
                name_employer = row['employer']['name']
                list_id.append(employer_id)
                cur.execute(
                    """
                    INSERT INTO company (name_employer)
                    VALUES (%s)
                    RETURNING employer_id
                    """,
                    (name_employer,)
                )
                employer_id = cur.fetchone()[0]

            vacancies_id = row['id']
            name_vacancies = row['name']
            salary = row['salary'].get('from') if row['salary'] is not None else None
            alternate_url = row['alternate_url']
            cur.execute(
                """
                INSERT INTO vacancies (employer_id, vacancies_id, name_vacancies, salary, alternate_url)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (employer_id, vacancies_id, name_vacancies, salary, alternate_url)
            )

    conn.commit()
    conn.close()
