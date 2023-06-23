# Specify the base image for the Docker container
FROM prefecthq/prefect:latest-python3.9

# Set the working directory inside the container to /app
WORKDIR /app

# Copy the cluster model source code and dependencies to the /app/cluster-model directory
COPY src/cluster-model /app/cluster-model
COPY src/deps /app/cluster-model

# Install Python packages psycopg2-binary, pandas, sqlalchemy, and scikit-learn using pip,
# with --no-cache-dir option to avoid caching downloaded packages
RUN pip install --no-cache-dir psycopg2-binary pandas sqlalchemy scikit-learn

# Run the cluster model script
CMD python /app/cluster-model/cluster_model.py
