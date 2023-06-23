# Specify the base image for the Docker container
FROM prefecthq/prefect:latest-python3.9

# Set the working directory inside the container to /app
WORKDIR /app

# Copy the data pipeline source code, dependencies, and data to the /app/datapipeline directory
COPY src/datapipeline /app/datapipeline
COPY src/deps /app/datapipeline
COPY data /app/datapipeline

# Install Python packages psycopg2-binary and pandas using pip, with --no-cache-dir option to 
# avoid caching downloaded packages
RUN pip install --no-cache-dir psycopg2-binary pandas

# Run the data pipeline script
CMD python /app/datapipeline/data_pipeline.py
