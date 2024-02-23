from sqlalchemy import create_engine

def main(engine, dataframe) -> None:
    session = engine.connect()
    try:
        dataframe.to_sql('monthly_salary_per_hour', con=session, if_exists='replace', index=False)
        print("Data loaded successfully.")
    except Exception as e:
        print(f"Error: {e}")