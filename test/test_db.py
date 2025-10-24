def test_add_positive_numbers():
    # Arrange / Act / Assert
    assert 1 + 1 == 2


def test_db(db):
    print(type(db))
    assert db.test()
