from src.config import config
from src.dbmanager import DBManager
from src.functions import db_create, get_company_vacancies, insert_data_to_tables

params = config()
EMPLOYERS_IDS = ['3529', '80', '4181', '2492', '4023', '3783', '6591', '3388', '599', '78638']


def main():
    database_name = 'headhunter'
    db_create(database_name, **params)  # Создание БД.
    db = DBManager()
    db.drop_table()
    db.create_table()
    for employer_id in EMPLOYERS_IDS:
        get_company_vacancies(employer_id)
        vacancies = get_company_vacancies(employer_id)
        insert_data_to_tables(vacancies, database_name, params)
    print('Список всех компаний и количество вакансий у каждой компании.')
    for i in db.get_employers_and_vacancies_count():
        print(i[0], i[1])
    print()
    print('Вакансии с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию.')
    for i in db.get_all_vacancies():
        if i[2] is None:
            print(i[0], i[1], i[3])
        else:
            print(i[0], i[1], i[2], i[3])
    print()
    print('Средняя зарплата по вакансиям.')
    print(db.get_avg_salary()[0])
    print()
    print('Список всех вакансий, у которых зарплата выше средней по всем вакансиям.')
    for i in db.get_vacancies_with_higher_salary():
        print(i[2], i[3], i[4])
    print()
    for i in db.get_vacancies_with_keyword(input('Введите название вакансии: ')):
        print(i[2], i[3], i[4])


if __name__ == '__main__':
    main()
