FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pipy.python.org --no-cache-dir

COPY flows /opt/prefect/flows
COPY data_yellow /opt/prefect/data_yellow