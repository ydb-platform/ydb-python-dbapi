import ydb_dbapi as dbapi


def main() -> None:
    connection = dbapi.connect(
        host="localhost",
        port=2136,
        database="/local",
    )

    print(f"Existing tables: {connection.get_table_names()}")
    print(f"Existing views: {connection.get_view_names()}")
    # TODO: fill example

    connection.close()


if __name__ == "__main__":
    main()
