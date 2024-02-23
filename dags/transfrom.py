import pandas as pd
from datetime import datetime

def main(engine, table_name_1, table_name_2) -> pd.DataFrame:
    session = engine.connect()

    employees_df = pd.read_sql(f'SELECT * FROM raw_{table_name_1}', con=session)
    timesheets_df = pd.read_sql(f'SELECT * FROM raw_{table_name_2}', con=session)

    employees_df['join_date'] = pd.to_datetime(employees_df['join_date'], format='%Y-%m-%d')
    employees_df['resign_date'] = pd.to_datetime(employees_df['resign_date'], format='%Y-%m-%d', errors='coerce')
    timesheets_df['date'] = pd.to_datetime(timesheets_df['date'], format='%Y-%m-%d')

    current_employees_df = employees_df[~employees_df['resign_date'].isnull()]
    timesheets_df = timesheets_df[~(timesheets_df['checkin'].isnull()) & ~(timesheets_df['checkout'].isnull())]

    monthly_salary_per_hour = pd.merge(current_employees_df, timesheets_df, left_on='employe_id', right_on='employee_id')
    monthly_salary_per_hour['hours_worked'] = monthly_salary_per_hour.apply(
        lambda row: (pd.to_datetime(row['checkout'], format='%H:%M:%S') - pd.to_datetime(row['checkin'], format='%H:%M:%S')).seconds / 3600,
        axis=1
    )

    monthly_salary_per_hour['monthly_salary'] = monthly_salary_per_hour['salary'] * monthly_salary_per_hour['hours_worked']

    monthly_salary_per_hour = monthly_salary_per_hour.groupby(['branch_id', pd.Grouper(key='date', freq='M')]) \
        .agg({'monthly_salary': 'sum', 'hours_worked': 'sum'}) \
        .reset_index()

    monthly_salary_per_hour['salary_per_hour'] = (monthly_salary_per_hour['monthly_salary'] / (monthly_salary_per_hour['hours_worked'] * 160)).round()
    monthly_salary_per_hour['salary_per_hour'] = monthly_salary_per_hour['salary_per_hour'].astype(int)


    monthly_salary_per_hour['year'] = monthly_salary_per_hour['date'].dt.year
    monthly_salary_per_hour['month'] = pd.to_datetime(monthly_salary_per_hour['date'], format='%m').dt.strftime('%B')


    monthly_salary_per_hour = monthly_salary_per_hour[['year', 'month', 'branch_id', 'salary_per_hour']]

    return monthly_salary_per_hour
