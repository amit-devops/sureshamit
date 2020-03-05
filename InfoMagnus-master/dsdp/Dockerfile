FROM python:3.7

RUN mkdir -p /var/www/dsdp

# Update working directory
WORKDIR /var/www/dsdp

COPY requirements.txt ./requirements.txt

# Install explicitly because of  segment fault with psycopg2.
RUN pip3 install --no-binary :all: psycopg2

# Install the Python libraries
RUN pip3 install --no-cache-dir -r requirements.txt

# copy everything from this directory to server/flask docker container
COPY . /var/www/dsdp/

# Give execute permission to below file, so that the script can be executed by docker.
RUN chmod +x /var/www/dsdp/entrypoint.sh

# run server
CMD ["./entrypoint.sh"]
