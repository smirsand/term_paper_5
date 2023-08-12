import psycopg2

from src.config import config


class DBManager:
    ''' Класс для работы с БД.'''

    def __init__(self):
        params = config()
        self.conn = psycopg2.connect(**params)

    def get_companies_and_vacancies_count(self):
        ''' Получает список всех компаний и количество вакансий у каждой компании.'''

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT company.name_employer, COUNT(vacancies.vacancies_id) as vacancies_count "
                "FROM company LEFT JOIN vacancies ON company.employer_id = vacancies.employer_id "
                "GROUP BY company.name_employer")
            rows = cur.fetchall()
            for row in rows:
                print(row)

    def get_all_vacancies(self):

        '''
        Получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию.
        '''

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT vacancies.name_vacancies, company.name_employer, vacancies.salary, vacancies.vacancies_id "
                "FROM company INNER JOIN vacancies ON company.employer_id = vacancies.employer_id")
            rows = cur.fetchall()
            for row in rows:
                print(row)

    def get_avg_salary(self):
        ''' Получает среднюю зарплату по вакансиям.'''

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT SUM(vacancies.salary) / COUNT(vacancies.vacancies_id) AS average_salary FROM vacancies")
            rows = cur.fetchall()
            for row in rows:
                print(row)

    def get_vacancies_with_higher_salary(self):
        ''' Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.'''

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM vacancies WHERE vacancies.salary > (SELECT SUM(salary) / COUNT(vacancies_id) "
                "FROM vacancies)")
            rows = cur.fetchall()
            for row in rows:
                print(row)

    def get_vacancies_with_keyword(self):
        ''' Получает список всех вакансий, в названии которых содержатся переданные в метод слова, например “java”.'''

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM vacancies WHERE name_vacancies LIKE '%Python%'")
            rows = cur.fetchall()
            for row in rows:
                print(row)

    def create_table(database_name: str, params: dict):
        """ Создание таблиц для сохранения данных о компаниях и вакансиях."""

        conn = psycopg2.connect(dbname=database_name, **params)
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE company(
                employer_id SERIAL PRIMARY KEY,
                name_employer varchar(100) NOT NULL
                )
                """)

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE vacancies(
                employer_id INT REFERENCES company(employer_id),
                vacancies_id int,
                name_vacancies varchar(100) NOT NULL,
                salary int,
                alternate_url varchar NOT NULL
                )
                """)

    def drop_table(self, database_name, **params):
        conn = psycopg2.connect(dbname='postgres', **params)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f'DROP DATABASE {database_name}')

        conn.close()
