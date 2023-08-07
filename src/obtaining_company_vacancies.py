import json

import requests


def get_company_vacancies(company_id):
    url = f'https://api.hh.ru/vacancies'
    params = {'employer_id': company_id, 'per_page': 100}
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        vacancies = data['items']
        for vacancy in vacancies:
            print(vacancy)
            return vacancy
    else:
        print(f"Ошибка при запросе данных: {response.status_code}")
