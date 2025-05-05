import pytest
from src.database.db_manager import DBManager

@pytest.fixture
def db_manager():
    return DBManager()

def test_get_companies_and_vacancies_count(db_manager):
    result = db_manager.get_companies_and_vacancies_count()
    assert isinstance(result, list)
    if result:
        assert "name" in result[0]
        assert "count" in result[0]

def test_get_avg_salary(db_manager):
    avg = db_manager.get_avg_salary()
    assert isinstance(avg, float)
