'''
This  User Interface service is responsible for providing an front end display to the user of the operational updates 
relating to the data engineering task, the output after ingesting, processing and aggregating the humidity data and 
finally the clustered humidity data.

'''

# Importation of the relevant libraries
import pandas, prefect, sqlalchemy, datetime
import connConfiguration
from bokeh import plotting as bokeh_plotting
from bokeh import models as bokeh_model
from prefect import schedules as ps
from prefect import executors as pe

def create_engine_connection():
    # Create a SQLAlchemy engine using the provided connection configuration
    db = sqlalchemy.create_engine(
        f'postgresql+psycopg2://{connConfiguration.login}:{connConfiguration.pw}@{connConfiguration.host}/{connConfiguration.database}', 
        pool_recycle=3600,  # Set the maximum time (in seconds) that a connection can remain idle in the pool before it is closed and recycled
        echo=False  # Determine whether SQL statements should be logged
    ).connect()  # Connect to the database using the engine
    # Return the database connection object
    return db

def close_connection(DBconn):
    # Close the provided database connection
    DBconn.close()

def create_data_table(temp_df, columns):
    # Create a ColumnDataSource object using the provided temporary DataFrame
    source = bokeh_model.ColumnDataSource(temp_df)
    # Create a DataTable object using the ColumnDataSource and specified columns
    dataset = bokeh_model.DataTable(source=source, columns=columns, width=900, height=900, auto_edit=False)
    # Return the created DataTable object
    return dataset

def create_columns(fields, titles):
    # Create an empty list to store the TableColumn objects
    columns = []
    # Iterate over the fields and titles simultaneously using zip()
    for field, title in zip(fields, titles):
        # Create a TableColumn object with the current field and title
        column = bokeh_model.TableColumn(field=field, title=title)
        # Append the column to the list of columns
        columns.append(column)
    # Return the list of created TableColumn objects
    return columns


@prefect.task(max_retries=4, retry_delay=datetime.timedelta(seconds=1))
def clusteredData(operations):
    # Establish a database connection
    DBconn = create_engine_connection()
    # Define the SQL query
    postgreSQL = """select tail_num, origin, dest, week, humidity, rank from humidity_rank;"""
    # Execute the SQL query and store the results in a DataFrame
    temp_df = pandas.read_sql_query(postgreSQL, con=DBconn)
    # Close the database connection
    close_connection(DBconn)
    # Define the fields and titles for the columns
    fields = ["tail_num", "origin", "dest", "week", "humidity", "rank"]
    titles = ["Tail Number", "Origin Airport", "Destination Airport", "Week", "Humidity", "Humidity Level"]
    # Create the TableColumn objects
    columns = create_columns(fields, titles)
    # Create the DataTable object
    dataset = create_data_table(temp_df, columns)
    # Set the output file for the Bokeh plot
    bokeh_plotting.output_file("html-files/clustered-table.html")
    # Display the DataTable
    bokeh_plotting.show(dataset)

@prefect.task(max_retries=4, retry_delay=datetime.timedelta(seconds=1))
def dataPipelineData(operations):
    # Establish a database connection
    DBconn = create_engine_connection()
    # Define the SQL query
    postgreSQL = "select tail_num, origin, dest, week, cast(date as text), humidity from flights;"
    # Execute the SQL query and store the results in a DataFrame
    temp_df = pandas.read_sql_query(postgreSQL, con=DBconn)
    # Close the database connection
    close_connection(DBconn)
    # Define the fields and titles for the columns
    fields = ["tail_num", "origin", "dest", "week", "date", "humidity"]
    titles = ["Tail Number", "Origin Airport", "Destination Airport", "Week", "Date", "Humidity"]
    # Create the TableColumn objects
    columns = create_columns(fields, titles)
    # Create the DataTable object
    dataset = create_data_table(temp_df, columns)
    # Set the output file for the Bokeh plot
    bokeh_plotting.output_file("html-files/datapipeline-table.html")
    # Display the DataTable
    bokeh_plotting.show(dataset)

@prefect.task(max_retries=4, retry_delay=datetime.timedelta(seconds=1))
def message_info():
    # Establish a database connection
    DBconn = create_engine_connection()
    # Define the SQL query
    postgreSQL = "select id, update, cast(loaddate as text) from operations;"
    # Execute the SQL query and store the results in a DataFrame
    temp_df = pandas.read_sql_query(postgreSQL, con=DBconn)
    # Close the database connection
    close_connection(DBconn)
    # Define the fields and titles for the columns
    fields = ["id", "update", "loaddate"]
    titles = ["Task Number", "Update on Task", "Log Data"]
    # Create the TableColumn objects
    columns = create_columns(fields, titles)
    # Create the DataTable object
    dataset = create_data_table(temp_df, columns)
    # Set the output file for the Bokeh plot
    bokeh_plotting.output_file("html-files/operational-updates.html")
    # Display the DataTable
    bokeh_plotting.show(dataset)
    
def main():
    # Get the current UTC time
    current_time = datetime.datetime.utcnow()

    # Define the start delay and time span in minutes
    start_delay = 1
    time_span_minutes = start_delay + 1

    # Calculate the initiation time and duration based on the current time, start delay, and time span
    initiation_time = current_time + datetime.timedelta(seconds=start_delay)
    duration = datetime.timedelta(minutes=time_span_minutes)

    # Create an IntervalSchedule with the calculated initiation time and duration
    varA = ps.IntervalSchedule(start_date=initiation_time, interval=duration)

    # Define the Prefect flow
    with prefect.Flow("User Interface Service", schedule=varA) as flow:
        # Execute the message_info task and store the output in outputA
        outputA = message_info()

        # Execute the dataPipelineData task using outputA as input and update outputA
        outputA = dataPipelineData(outputA)

        # Execute the clusteredData task using outputA as input and update outputA
        outputA = clusteredData(outputA)

        # Print the final outputA
        print(outputA)

    # Run the flow using a LocalDaskExecutor
    flow.run(executor=pe.LocalDaskExecutor())


if __name__ == "__main__":
    main()

