'''
The cluster model shall use the processed data to present analysis on 
whether the humidity level at the origin airport was either good, bad or moderate for an aeroplane to takeoff.

'''
# Importation of the Relevant Libraries
import time, prefect, datetime, psycopg2, pandas
import connConfiguration
from sklearn import cluster as sklearn_cluster
from prefect import schedules as ps
from prefect import executors as pe

# Function to create a database connection
def create_connection():
    # Establish a connection to the PostgreSQL database using psycopg2 library
    # and the connection configuration values
    db_conn = psycopg2.connect(
        host=connConfiguration.host,   # Specify the host address of the database server
        dbname=connConfiguration.database,   # Specify the name of the database to connect to
        user=connConfiguration.login,   # Specify the username for authentication
        password=connConfiguration.pw   # Specify the password for authentication
    )
    # Return the established database connection
    return db_conn

# Function to initialize data
@prefect.task(max_retries=4, retry_delay=datetime.timedelta(seconds=1))
def initializeData():
    # Establish a database connection using the create_connection() function and create a cursor object
    with create_connection().cursor() as db_object:
        try:
            # Execute a SQL query to create a table named 'humidity_rank' if it doesn't already exist
            db_object.execute("create table IF NOT EXISTS humidity_rank (tail_num varchar(20), origin varchar(15), dest varchar(15), week int, humidity varchar(20), rank varchar(15));")
        except:
            # Rollback the transaction if an exception occurs during the query execution
            db_object.connection.rollback()
        # Commit the changes made to the database
        db_object.connection.commit()

# Function to check a condition
def check_condition(criteria):
    # Create a database connection using the 'create_connection' function and use it as a context manager
    with create_connection() as db_conn:
        # Create a cursor using the database connection and use it as a context manager
        with db_conn.cursor() as db_1:
            # Execute an SQL query to retrieve the latest 'id' from the 'operations' table
            db_1.execute("SELECT id FROM operations ORDER BY id DESC LIMIT 1")
            # Fetch the result of the query and return the value of the first column
            return db_1.fetchone()[0] != criteria

# Function to store message information
def message_info(update):
    # Get the current datetime and format it as a string
    var = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # Establish a database connection using the create_connection() function
    db_conn = create_connection()
    # Execute an SQL query to insert the message update and current datetime into the 'operations' table
    # using a cursor object within a context manager
    with db_conn, db_conn.cursor() as db_1:
        db_1.execute("INSERT INTO operations (update, loaddate) VALUES (%s, %s)", (update, var))

@prefect.task(max_retries=4, retry_delay=datetime.timedelta(seconds=1))
def dataIngestion():
    # Continue looping while the condition is true
    while check_condition(7):
        print("Please wait, the datapipeline has NOT been concluded")
        time.sleep(60)
    # Log a message indicating the start of data ingestion
    message_info("Start of Cluster Model Data Ingestion")
    # Create the engine URL for connecting to the PostgreSQL database
    engine_url = f'postgresql+psycopg2://{connConfiguration.login}:{connConfiguration.pw}@{connConfiguration.host}/{connConfiguration.database}'
    # Execute the SQL query and retrieve the results as a pandas DataFrame
    return pandas.read_sql_query("""
        select tail_num, origin, dest, week, round(avg(humidity), 2) as humidity
        from flights
        group by tail_num, origin, dest, week
        order by tail_num, origin, dest, week;
    """, con=engine_url)

@prefect.task(max_retries=4, retry_delay=datetime.timedelta(seconds=1))
def clusterData(record):
    # Drop rows with missing values in the 'humidity' column
    record.dropna(subset=['humidity'], inplace=True)
    # Perform K-means clustering on the record DataFrame
    # Set the number of clusters to 3 and use a fixed random state for reproducibility
    record['cluster'] = sklearn_cluster.KMeans(n_clusters=3, random_state=42).fit(record.drop(columns=["tail_num","origin","dest"])).labels_
    # Return the modified record DataFrame
    return record

@prefect.task(max_retries=4, retry_delay=datetime.timedelta(seconds=1))
def data_processing(record):
    # Create a DataFrame to store cluster metadata
    clusterMetadata = pandas.DataFrame({"cluster": [0, 1, 2], "humidity": [record[record.cluster == i]["humidity"].mean() for i in range(3)]})
    # Sort the cluster metadata DataFrame by the 'humidity' column
    clusterMetadata = clusterMetadata.sort_values(by='humidity') 
    # Assign ranks ('Good', 'Moderate', 'Bad') to the cluster metadata DataFrame
    clusterMetadata['rank'] = ['Good', 'Moderate', 'Bad']
    # Assign ranks to the 'record' DataFrame based on cluster assignments
    record['rank'] = record.apply(lambda x: clusterMetadata[clusterMetadata.cluster == x['cluster']]['rank'].iloc[0], axis=1)
    # Remove the 'cluster' column from the 'record' DataFrame and return the modified DataFrame
    return record.drop(columns="cluster")

@prefect.task(max_retries=4, retry_delay=datetime.timedelta(seconds=1))
def data_integration(record):
    # Create a database connection
    db_conn = create_connection()
    # Create a database cursor object
    db_object = db_conn.cursor()
    # SQL query for inserting data into the 'humidity_rank' table
    sql = "INSERT INTO humidity_rank (tail_num, origin, dest, week, humidity, rank) VALUES (%s, %s, %s, %s, %s, %s);"
    # Batch size for executing batch inserts
    batch_size = 1000
    # Initialize an empty batch
    batch = []
    # Iterate over the rows of the 'record' DataFrame
    for index, row in record.iterrows():
        # Extract the values from the current row
        values = (row[0], row[1], row[2], row[3], row[4], row[5])
        # Add the values to the batch
        batch.append(values)
        # Check if the batch size has been reached
        if len(batch) >= batch_size:
            # Execute the batch insert
            execute_batch_insert(db_object, sql, batch)
            # Reset the batch
            batch = []
    # Check if there are remaining values in the batch
    if len(batch) > 0:
        # Execute the remaining batch insert
        execute_batch_insert(db_object, sql, batch)
    # Commit the changes to the database
    db_conn.commit()
    # Close the database connection
    db_conn.close()
    # Log a message indicating the conclusion of data integration
    message_info("Data Integration Concluded")
    message_info("All data engineering processes concluded.")
    # Return a string indicating the completion of the task
    return "Done"

def execute_batch_insert(cursor, sql, values_batch):
    try:
        # Execute the batch insert using the executemany() method of the cursor
        cursor.executemany(sql, values_batch)
    except:
        # If an exception occurs during the batch insert, roll back the changes
        cursor.connection.rollback()

def main():
    # Define the interval for the schedule
    interval = datetime.timedelta(minutes=2) 
    # Create an interval schedule
    schedule = ps.IntervalSchedule(
        start_date=datetime.datetime.utcnow() + datetime.timedelta(seconds=1),
        interval=interval
    )
    # Create a Prefect flow with the name "cluster-model" and the defined schedule
    with prefect.Flow("cluster-model", schedule=schedule) as flow:
        # Task: initializeData
        initializeData() 
        # Task: dataIngestion
        record = dataIngestion()
        # Task: clusterData
        record = clusterData(record)
        # Task: data_processing
        record = data_processing(record)
        # Task: data_integration
        data_integration(record)
    # Run the Prefect flow using a local Dask executor
    flow.run(executor=pe.LocalDaskExecutor())

if __name__ == "__main__":
    main()


