# Specify the base image for the Docker container
FROM prefecthq/prefect:latest-python3.9

# Update the package repository and install Nginx
RUN apt-get update
RUN apt-get install -y nginx

# Install Python packages using pip
RUN pip install bokeh psycopg2-binary pandas sqlalchemy

# Copy the user interface source code to the Nginx HTML directory
COPY src/user-interface /usr/share/nginx/html
COPY src/deps /usr/share/nginx/html

# Copy the Nginx settings configuration file
RUN cp /usr/share/nginx/html/nginx/settings.conf /etc/nginx/conf.d/settings.conf

# Start the Nginx service and run the user interface operation script
CMD service nginx start && cd /usr/share/nginx/html/ && python user-interface-operation.py
