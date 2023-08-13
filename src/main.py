from src.config import config
from src.dbmanager import DBManager
from src.functions import db_create

params = config()
EMPLOYERS_IDS = ['3529', '80', '4181', '2492', '4023', '3783', '6591', '3388', '599', '78638']


def main():
    database_name = 'headhanter'
    db = DBManager()
    try:
        db.drop_table(database_name, **params)  # Удаление 'headhanter.'
    except Exception:
        None

    db_create()  # Создание БД.


if __name__ == '__main__':
    main()
