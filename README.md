# Batch Processing Application for Analyzing Flights Data

## Project Brief:
This project intends to implement a data architecture for a data-intensive batch processing application that analyses the humidity of aircraft at their origin airport, to advise Aviation Officials on the safety of planes departing from the origin airport. The project uses the humidity value to cluster data based on whether it would be (Good, Moderate, or Bad) for aircraft to take off to their destination flight. The dataset has approximately 1.7 million data records and uses the Prefect tool for workflow automation. To view a live demonstration of the project [Visit this link](https://www.youtube.com/watch?v=K6rpIQ7e2CE)

## Data Processing Operational Architecture:

![VejendraDattArchitecture](https://github.com/VejAlDatt/flights-project/assets/126171063/0b58f703-f11c-4af9-9250-7535c2f6d5a1)

## Main Steps of the Data Engineering Implementation:

The following are the important steps the data engineering project underwent:  

1. The Data Pipeline begins by ingesting data from selected columns in the `flight_weather.csv` file, it then concatenates the data records into a single dataframe and logs to the operations table the status of the process.
2. The data processing task would then take the ingested data and perform several transformation activities on the data, such as dropping records with `NaN` values, calculating the mean of the humidity column, determining the approximate week based on the day of the month, etc. It also logs this process to the operations table.
3. The data integration task then takes the processed records as input. It establishes a database connection and iterates over the records. It appends non-null tuple values to a list (rows) and processes the list in batches to efficiently insert the rows into the database table named `flights`. It also logs this process to the operations table.
4. In the Cluster Model, the data is then fetched from the `flights` table, and the average humidity for each flight is calculated and stored in a pandas dataframe.
5. The data processing then begins by clustering the data based on their humidity value. They are then assigned a rank (Good, Moderate, bad) based on their humidity level that details the severity for planes to depart from their origin airport. The processed data is then stored in the humidity rank table. The process is also logged into the operations table.
6. Lastly, there is the user interface service that’s responsible for displaying operational updates. It presents the output after ingesting, processing, and aggregating the humidity data.

## Steps to run the data engineering project on your local system:
You could either choose to view the aformentioned live demonstration for setting up and running this project via this [YouTube link](https://www.youtube.com/watch?v=K6rpIQ7e2CE) or follow the step-by-step guidelines provided below:

1. Ensure that your docker engine is stable and is running without problems, given that you have already downloaded Docker Desktop and Docker Compose.
2. Download this zipped project from GitHub, by clicking on the green button above named ‘Code’ and then further clicking on the option entitled ‘Download Zip’.
3. Once downloaded, extract the files to your local system.
4. Once you extracted the files, you may notice the data folder is missing the Dataset (This is because GitHub has a limitation of 100 MB on file uploads, so we will need to download our CSV Dataset file from Kaggle itself.
5. Use this [Kaggle link](https://www.kaggle.com/datasets/shubhamkumartomar/flight-weather-dataset) to be redirected to the Kaggle page which has the dataset. Ensure you log in with your credentials to be allowed the privilege to download the CSV file. Click the download button to deploy the file to your system.
6. Once downloaded, extract or unzip the file. Copy the CSV file and paste it inside the ‘data’ folder of the extracted GitHub repository you downloaded which was entitled ‘flights-project’.
7. Open Command Prompt on your system.
8. Navigate to the previously downloaded and extracted project ‘flights-project’ via the command prompt.
9. Once inside enter the command 'docker-compose build ' and press enter.
10. Once all the respective docker builds are finished, enter the command ‘docker-compose up -d’ and press enter.
11. Once all the containers have started, open your browser and enter in your address bar ‘localhost:8080’.
12. You could now interact with the webpage to view the different operations of the data engineering project.
