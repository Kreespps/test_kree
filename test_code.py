import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, select, func, exc
import mariadb
import sys
#import shutil
#from pyspark.sql import SparkSession
from datetime import datetime, timedelta, date
import time
from sqlalchemy.dialects.mysql import insert

def connect_db():
    try:
        conn = mariadb.connect(
            user="root",
            password="Teetee11045",
            host="localhost",
            port=3306,
            database="database_01"

        )
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)
    cur = conn.cursor()

def create_db_engine():
    user="root"
    password="Teetee11045"
    host="localhost"
    port=3306
    database="database_01"
    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}")
    return engine

#if the file ingest into hdfs the process should be done with pyspark
#if the file ingest into workbench the process should be done with pandas
def main():
    #and i suppose this process was on hdfs

    '''spark = SparkSession \
        .builder \
        .appName("data and visualization enginner") \
        .getOrCreate()
    #if the data was bigger than default config u can apply more specific config
    file_path = "project_code/file"
    df = spark.read.format("parquet")\
            .load(file_path)
    numberrow = df.count()
    return numberow'''

    #and i suppose this process was on workbench

        
    print("Executing load_data.py for day1")
    load_data("day1", source_data_path_day1, archive_path)
    
    print("Executing load_data.py for day2")

    load_data("day2", source_data_path_day2, archive_path)

def insert_into_table(day,df, table_name, engine):
    # Count rows before insert
    '''with engine.connect() as conn:
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=conn.engine)
        #rows_before_insert = conn.execute(select([func.count()]).select_from(table)).scalar()
        number_of_rows = conn.execute(f"SELECT count(*) FROM {table_name}")
    print(df.info())'''
    if day == 'day1':
        with engine.connect() as conn:
            df.to_sql(name=table_name, con=conn, if_exists='replace')   
    
    # Inserting data into the table
    else:
        with engine.connect() as conn:
            print(df.to_string())
            existing_data = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            print("Existing Data:")
            print(existing_data)

            # Check if both DataFrames are non-empty before concatenation
            if not df.empty and not existing_data.empty:
                # Identify and remove duplicates
                df_no_duplicates = pd.concat([df, existing_data], ignore_index=True).drop_duplicates(keep=False)

                # Exclude columns with all-NA values before concatenation
                df_no_duplicates = df_no_duplicates.dropna(axis=1, how='all')

                print("Unique Records:")
                print(df_no_duplicates)

                # Insert only unique records into the table
                df_no_duplicates.to_sql(name=table_name, con=conn, if_exists='append', index=False)
                print(f"Inserted {len(df_no_duplicates)} unique records into {table_name}")
            else:
                print("Either 'df' or 'existing_data' is empty. No concatenation and insertion.")

        #df.to_sql(name=table_name, con=conn, if_exists='append', method=insert_on_duplicate)   

        '''for i in range(len(df)):
            try:
                df.iloc[i:i+1].to_sql(name=table_name,if_exists='append',con = conn, index=False)
            except exc.IntegrityError as e:
                pass #or any other action'''
        #df.to_sql(name=table_name, con=conn, if_exists='replace')

        #this will append with no reason
        #df.to_sql(name=table_name, con=conn, if_exists='append', index=False,index_label=None)

        #df.to_sql(name=table_name, con=conn, if_exists='append', index=False, method=insert_on_conflict_update)

        #df.to_sql(name=table_name, con=conn, if_exists='replace', index=False, method=insert_on_conflict_update)
        #df_conflict.to_sql(name="conflict_table", con=conn, if_exists="append", method=insert_on_conflict_nothing)  

    '''# Count rows after insert
    with engine.connect() as conn:
        rows_after_insert = conn.execute(select([func.count()]).select_from(table)).scalar()

    # Calculate the number of rows inserted
    rows_inserted = rows_after_insert - rows_before_insert
    print(f"{rows_inserted} rows inserted into {table_name}")'''
def clean_postal_code(postal_code):
    # Remove non-numeric characters
    cleaned_code = ''.join(char for char in str(postal_code) if char.isdigit())
    
    # Ensure the cleaned code is not more than 10 digits
    if len(cleaned_code) <= 10:
        return cleaned_code
    else:
        # Truncate to the first 10 digits
        return cleaned_code[:9]
def insert_on_conflict_update(table, conn, keys, data_iter):
    # update columns on primary key conflict
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = (
        insert(table.table)
        .values(data)
    )

    # Check if the column exists in the table
    if 'item_description' in stmt.table.c:
        stmt = stmt.on_duplicate_key_update(item_description=stmt.inserted.item_description)
    else:
        print("Column 'item_description' not found in the table.")

    result = conn.execute(stmt)
    return result.rowcount
def clean_item_description(item_description):
    # Remove non-numeric characters
    cleaned_code = ''.join(char for char in str(item_description) if char.isdigit())
    
    # Ensure the cleaned code is not more than 10 digits
    if len(cleaned_code) <= 50:
        return cleaned_code
    else:
        # Truncate to the first 10 digits
        return cleaned_code[:49]
def delete_all_data_from_table(table_name, engine):
    # Connect to the database
    with engine.connect() as conn:
        # Reflect the table structure
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine)

        # Execute DELETE statement without any conditions
        conn.execute(table.delete())

def insert_on_duplicate(table, conn, keys, data_iter):
    insert_stmt = insert(table.table).values(list(data_iter))
    on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(insert_stmt.inserted)
    conn.execute(on_duplicate_key_stmt)

def load_data(day, source_path, archive_path):
    engine = create_db_engine()
    #df = pd.read_sql("SELECT * FROM customer ", engine)
    #print(df.to_string())

    for file_name in os.listdir(source_path):
        if file_name.endswith(".csv"):

            file_path = os.path.join(source_path, file_name)
            print(file_path)

            # Read the CSV file using pandas
            
            if "item" in file_name:
                df = pd.read_csv(file_path,dtype = item_schema)
                df['item_description'] = df['item_description'].apply(clean_item_description)

                insert_into_table(day,df, "item", engine)
                print(f"insert {file_name}")
            elif "customer" in file_name:
                df = pd.read_csv(file_path,dtype = customer_schema)
                df["postal_code"] = df["postal_code"].fillna("None")
                df['postal_code'] = df['postal_code'].apply(clean_postal_code)

                insert_into_table(day,df, "customer", engine)
                print(f"insert {file_name}")
                
            elif "order_headers" in file_name:
                df = pd.read_csv(file_path)
                df['order_date'] = pd.to_datetime(df['order_date'])

                insert_into_table(day,df, "orders_header", engine)
                print(f"insert {file_name}")
               
            elif "order_lines" in file_name:
                df = pd.read_csv(file_path)
                print(df.info())
                print(df.dtypes)

                df['ship_date'] = pd.to_datetime(df['ship_date'])
                df['promise_date'] = pd.to_datetime(df['promise_date'])
                print(df.dtypes)

                insert_into_table(day,df, "orderline", engine)

                print(f"insert {file_name}")

            archive_file_path = os.path.join(archive_path, f"{day}_{file_name}")
            os.rename(file_path, archive_file_path)
def read_table_into_dataframe(table_name, engine):
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, engine)
    return df
def output(table_name):
    engine = create_db_engine()
    with engine.connect() as conn:
        print('start create out put')
        orderline_df = read_table_into_dataframe('orderline', engine)
        ordershead_df = read_table_into_dataframe('orders_header', engine)
        #print('ordershead_df before:')
        #print(ordershead_df.info())
        selected_columns = ['order_id', 'customer_id']
        ordershead_df = ordershead_df[selected_columns]
        #print('orderline_df:')
        #print(orderline_df.to_string())
        #print('ordershead_df:')
        #print(ordershead_df.to_string())

        result_df = pd.merge(orderline_df, ordershead_df, on='order_id', how='left')
        print(result_df.to_string())

        result_df.to_sql(name=table_name, con=conn, if_exists='replace', index=False)
        result_df.to_parquet('output/result_df.parquet') 
if __name__ == '__main__':
    source_data_path_day1 = "day1_files/"
    source_data_path_day2 = "day2_files/"    
    archive_path = "archive/"
    item_schema = {
        'item_id': 'int',
        'item_description': 'str',
        'item_status': 'str'
    }

    # Define the customer table schema
    customer_schema = {
        'customer_id': 'int',
        'customer_number': 'str',
        'customer_name': 'str',
        'address': 'str',
        'postal_code': 'str',
        'city': 'str',
        'country': 'str',
        'country_code': 'str',
        'telephone': 'str'
    }

    # Define the orders_header table schema
    orders_header_schema = {
        'order_id': 'int',
        'order_number': 'str',
        'order_date': 'date',
        'customer_id': 'int',
        'order_status': 'str',
        'currency': 'str'
    }

    # Define the orderline table schema
    orderline_schema = {
        'orderline_id': 'int',
        'order_id': 'int',
        'item_id': 'int',
        'ship_date': 'datetime',
        'promise_date': 'datetime',
        'ordered_quantity': 'int'
    }
    #start time
    start_time = time.time()
    start_datetime = datetime.now()
    formatted_start_datetime = start_datetime.strftime('%m/%d/%Y')
    print(formatted_start_datetime)

    main()
    table_output = 'sale_history'
    output(table_output)
    #end time
    end_time = time.time()
    end_datetime = datetime.now()
    formatted_end_datetime= end_datetime.strftime('%m/%d/%Y')

    print(formatted_start_datetime)
    elapsed_time = end_time - start_time

    # Convert elapsed time to minutes
    print(f"Elapsed Time (seconds): {elapsed_time}")
