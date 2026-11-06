import pytest
from auditor.sql import insert_sql


def test_inserts_are_idempotent_on_payment_id():
    sql = insert_sql("labels", ["payment_id", "is_fraud", "scenario"])
    assert sql == ("INSERT INTO labels (payment_id, is_fraud, scenario) VALUES (%s, %s, %s) "
                   "ON CONFLICT (payment_id) DO NOTHING")


def test_json_columns_are_cast():
    sql = insert_sql("decisions", ["payment_id", "features"])
    assert "VALUES (%s, %s::jsonb)" in sql


def test_unknown_tables_and_columns_are_refused_so_names_never_come_from_data():
    with pytest.raises(ValueError):
        insert_sql("users; DROP TABLE decisions", ["payment_id"])
    with pytest.raises(ValueError):
        insert_sql("labels", ["payment_id", "evil"])
