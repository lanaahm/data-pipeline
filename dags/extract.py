import os
import pandas as pd
from time import time

def main(engine, csv_name) -> str:
    session = engine.connect()
    table_name = os.path.basename(csv_name).split('.')[0]
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    
    try:
        # Bulk insert the first chunk
        df = next(df_iter)
        df.head(0).to_sql(name=f'raw_{table_name}', con=session, if_exists='replace', index=False)
        df.to_sql(name=f'raw_{table_name}', con=session, if_exists='append', index=False)

        while True:
            t_start = time()
            df = next(df_iter)
            df.to_sql(name=f'raw_{table_name}', con=session, if_exists='append', index=False, method='multi')  # Bulk insertion
            t_end = time()  # Record end time
            
            # Print time taken to process and insert the chunk
            print('Insert chunk, time %.3f second' % (t_end - t_start))
    except StopIteration:
        print("Finished ingesting data into the postgres database")
    except Exception as e:
        print(f"Error: {e}")
    
    return table_name