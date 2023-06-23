'''
This data pipeline exclusively handles the ingestion, the storage, the pre-processing and integration of the 
humidity data of aircrafts from origin airport across the world. The data shall then be used later 
by the cluster model to determine the severity of humidity levels for aircraft to takeoff from 
their origin airports.

'''
# Importation of libraries 
import prefect, datetime, psycopg2, pandas
from prefect import schedules as ps
from prefect import executors as pe
from psycopg2 import extras as db
import connConfiguration

# Function to create a database connection
def create_connection():
    return psycopg2.connect(f"host='{connConfiguration.host}' dbname='{connConfiguration.database}' user='{connConfiguration.login}' password='{connConfiguration.pw}'")

# Function to close the database connection
def close_connection(db_conn):
    db_conn.close()

# Function to initialize the database schema
def initializeSchema():
    # Create a cursor object using the connection
    currentVar = create_connection().cursor()
    # SQL statement to create tables if they don't already exist
    postgreSQL = "CREATE TABLE IF NOT EXISTS flights (tail_num VARCHAR(30), origin VARCHAR(25), dest VARCHAR(25), week INT, date DATE, humidity DECIMAL); CREATE TABLE IF NOT EXISTS operations (id SERIAL, update VARCHAR(400), loaddate TIMESTAMP);"
    try:
        # Execute the SQL statement
        currentVar.execute(postgreSQL)
        
        # Commit the changes to the database
        currentVar.connection.commit()
    except:
        # Rollback the changes if an error occurs
        currentVar.connection.rollback()
    # Close the database connection
    currentVar.connection.close()

# Function to store message information in the database
def message_info(update):
    # Get the current date and time
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # Create a database connection
    db_conn = create_connection()
    # Use a context manager to automatically handle the database connection and cursor
    with db_conn, db_conn.cursor() as cursor:
        # Execute an SQL statement to insert the message information into the 'operations' table
        cursor.execute("INSERT INTO operations (update, loaddate) VALUES (%s, %s)", (update, now))
    # Close the database connection
    close_connection(db_conn)

@prefect.task(max_retries=4, retry_delay=datetime.timedelta(seconds=1))
def dataIngestion():
    # Log message indicating the initiation of the ingestion process
    message_info("Ingestion Process Initiated")

    # Define the specific columns to read only from the CSV file
    columns_to_read = ["TAIL_NUM", "ORIGIN", "DEST", "MONTH", "DAY_OF_MONTH", "YEAR", "RelativeHumidityOrigin"]

    # Define the data types for the columns
    dtype = {
        col: "string" if col in ["TAIL_NUM", "ORIGIN", "DEST"] else
        int if col in ["MONTH", "DAY_OF_MONTH", "YEAR"] else float for col in columns_to_read
    }

    # Define the chunk size and the filename
    chunksize = 10000
    filename = "datapipeline/flight_weather.csv"

    # Initialize an empty list to store data chunks
    data_chunks = []

    # Read the CSV file in chunks
    reader = pandas.read_csv(filename, usecols=columns_to_read, dtype=dtype, chunksize=chunksize)

    # Iterate over the chunks
    for i, chunk in enumerate(reader):
        # Append each chunk to the list
        data_chunks.append(chunk)
        
        # Log message indicating ongoing data ingestion, only for the first chunk
        if i == 0:
            message_info("Data Ingestion Ongoing")

    # Concatenate all the data chunks into a single dataframe
    result = pandas.concat(data_chunks, ignore_index=True)

    # Log message indicating the conclusion of the data ingestion process
    message_info("Data Ingestion Concluded")

    # Return the result
    return result

@prefect.task(max_retries=4, retry_delay=datetime.timedelta(seconds=1))
def data_processing(records):
    # Log message indicating the initiation of data processing
    message_info("Data Processing Initiated")

    # Drop records with 'NaN' values in the 'RelativeHumidityOrigin' column
    records = records.drop(records[records.RelativeHumidityOrigin == 'NaN'].index)

    # Define the columns to group by
    group_cols = ['TAIL_NUM', 'ORIGIN', 'DEST', 'YEAR', 'MONTH', 'DAY_OF_MONTH']

    # Group records by the defined columns and calculate the mean of 'RelativeHumidityOrigin'
    records = records.groupby(group_cols, as_index=False)['RelativeHumidityOrigin'].mean()

    # Round the 'RelativeHumidityOrigin' values to 2 decimal places
    records['RelativeHumidityOrigin'] = records['RelativeHumidityOrigin'].round(2)

    # Define a function to determine the week based on the 'DAY_OF_MONTH' value
    def determineWeek(day_of_month):
        quarters = [1, 2, 3, 4]
        thresholds = [7, 15, 22, 31]
        return next((q for q, t in zip(quarters, thresholds) if day_of_month <= t), 4)

    # Apply the 'determineWeek' function to calculate the 'Week' column
    records['Week'] = records['DAY_OF_MONTH'].apply(determineWeek)

    # Convert the 'YEAR', 'MONTH', and 'DAY_OF_MONTH' columns to a single 'Date' column of type datetime
    records['Date'] = pandas.to_datetime(records[['YEAR', 'MONTH', 'DAY_OF_MONTH']].astype(str).apply(lambda x: '-'.join(x), axis=1))

    # Drop the 'YEAR', 'MONTH', and 'DAY_OF_MONTH' columns
    records = records.drop(columns=['YEAR', 'MONTH', 'DAY_OF_MONTH'])

    # Sort the records by the 'Date' column
    records = records.sort_values(by='Date')

    # Log message indicating the conclusion of data processing
    message_info("Data Processing Concluded")

    # Return the processed records
    return records

@prefect.task(max_retries=4, retry_delay=datetime.timedelta(seconds=1))
def data_integration(records):
    # Log message indicating the start of data integration
    message_info("Data Integration Started")

    # Create a database connection
    db_conn = create_connection()
    db_obj = db_conn.cursor()

    # Initialize variables for batch processing
    rows = []
    batch_size = 1500
    total_rows = 0

    # Iterate over the records
    for i, tuple in records.iterrows():
        if not pandas.isnull(tuple[3]):
            # Append the non-null tuple values to the 'rows' list
            rows.append((tuple[0], tuple[1], tuple[2], tuple[3], tuple[4], tuple[5]))

        # Process the batch when it reaches the specified batch size
        if len(rows) >= batch_size:
            db.execute_values(
                db_obj,
                "INSERT INTO flights (tail_num, origin, dest, humidity, week, date) VALUES %s;",
                rows,
            )
            db_conn.commit()
            total_rows += len(rows)
            rows.clear()

    # Process the remaining rows if there are any
    if rows:
        db.execute_values(
            db_obj,
            "INSERT INTO flights (tail_num, origin, dest, humidity, week, date) VALUES %s;",
            rows,
        )
        db_conn.commit()
        total_rows += len(rows)

    # Close the database connection
    close_connection(db_conn)

    # Log message indicating the conclusion of data integration
    message_info(f"Data Integration of {total_rows} rows concluded")

    # Return a completion message
    return "Data Pipeline Activities Concluded"

def main():
    # Set the duration for the interval schedule
    duration = datetime.timedelta(minutes=58)

    # Get the current time and calculate the initiation time
    current_time = datetime.datetime.utcnow()
    initiation_time = current_time + datetime.timedelta(seconds=1)

    # Define the interval schedule using the initiation time and duration
    varA = ps.IntervalSchedule(
        start_date=initiation_time,
        interval=duration
    )

    # Create a Prefect flow named "Data Pipeline" with the defined schedule
    with prefect.Flow("Data Pipeline", schedule=varA) as flow:
        # Ingest data using the dataIngestion task
        records = dataIngestion()

        # Perform data processing using the data_processing task
        records = data_processing(records)

        # Integrate data into the database using the data_integration task
        data_integration(records)

    # Initialize the database schema using the initializeSchema function
    initializeSchema()

    # Run the Prefect flow using a local Dask executor
    flow.run(executor=pe.LocalDaskExecutor())

if __name__ == "__main__":
    main()

