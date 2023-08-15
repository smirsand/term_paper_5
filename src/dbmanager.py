import psycopg2

from src.config import config


class DBManager:
    """ Класс для работы с БД."""

    def __init__(self):
        params = config()
        self.conn = psycopg2.connect(database='headhunter', **params)

    def get_employers_and_vacancies_count(self):
        """ Получает список всех компаний и количество вакансий у каждой компании."""

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT employers.name_employer, COUNT(vacancies.vacancies_id) as vacancies_count "
                "FROM employers LEFT JOIN vacancies ON employers.employer_id = vacancies.employer_id "
                "GROUP BY employers.name_employer")
            rows = cur.fetchall()

            return rows

    def get_all_vacancies(self):
        """ Получает список всех вакансий с указанием названия компании,
         названия вакансии и зарплаты и ссылки на вакансию."""

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT employers.name_employer, vacancies.name_vacancies, vacancies.salary, vacancies.alternate_url "
                "FROM employers INNER JOIN vacancies ON employers.employer_id = vacancies.employer_id")
            rows = cur.fetchall()

            return rows

    def get_avg_salary(self):
        """ Получает среднюю зарплату по вакансиям."""

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT SUM(vacancies.salary) / COUNT(vacancies.vacancies_id) AS average_salary FROM vacancies")
            rows = cur.fetchall()

            return rows[0]

    def get_vacancies_with_higher_salary(self):
        """ Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM vacancies WHERE vacancies.salary > (SELECT SUM(salary) / COUNT(vacancies_id) "
                "FROM vacancies)")
            rows = cur.fetchall()
            return rows

    def get_vacancies_with_keyword(self, keyword):
        """ Получает список всех вакансий, в названии которых содержатся переданные в метод слова, например “java”."""

        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM vacancies WHERE name_vacancies LIKE '%{keyword}%'")
            rows = cur.fetchall()
            return rows

    def create_table(self):
        """ Создание таблиц для сохранения данных о компаниях и вакансиях."""

        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE employers(
                employer_id SERIAL PRIMARY KEY,
                name_employer varchar(100) NOT NULL
                )
                """)

        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE vacancies(
                employer_id INT REFERENCES employers(employer_id),
                vacancies_id int,
                name_vacancies varchar(100) NOT NULL,
                salary int,
                alternate_url varchar NOT NULL
                )
                """)

        self.conn.commit()

    def drop_table(self):
        """ Удаление таблиц."""

        with self.conn.cursor() as cur:
            cur.execute("""
                DROP TABLE IF EXISTS vacancies;
                DROP TABLE IF EXISTS employers;
                """)

        self.conn.commit()
