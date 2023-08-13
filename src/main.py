from src.config import config
from src.dbmanager import DBManager
from src.functions import db_create, get_company_vacancies, insert_data_to_tables

params = config()
EMPLOYERS_IDS = ['3529', '80', '4181', '2492', '4023', '3783', '6591', '3388', '599', '78638']


def main():
    database_name = 'headhanter'
    db_create(database_name, **params)  # Создание БД.
    db = DBManager()
    db.drop_table()
    db.create_table()
    for employer_id in EMPLOYERS_IDS:
        get_company_vacancies(employer_id)
        vacancies = get_company_vacancies(employer_id)
        insert_data_to_tables(vacancies, database_name, params)


if __name__ == '__main__':
    main()
