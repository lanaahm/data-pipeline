from sqlalchemy import create_engine

def create_engine_instance(params) -> create_engine:
    return create_engine(f"postgresql+psycopg2://{params['user']}:{params['password']}@{params['host']}/{params['dbname']}")

def create_database_if_not_exists(engine, dbname) -> None:
    try:
        connection = engine.connect()
        check_database = connection.execute("SELECT datname FROM pg_database")
        existing_databases = [d[0] for d in check_database]
        if dbname not in existing_databases:
            connection.execute("commit")
            connection.execute(f"CREATE DATABASE {dbname}")
    except Exception as e:
        print(f"Error: {e}")