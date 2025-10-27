def test_db(db):
    print(type(db))
    assert db.test()
