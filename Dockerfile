FROM python:3.10-slim
# Checkout and install dagster libraries needed to run the gRPC server
# exposing your repository to dagster-webserver and dagster-daemon, and to load the DagsterInstance
ENV DAGSTER_VERSION=1.5.11 DAGSTER_LIBS_VERSION=0.21.11
# Run dagster gRPC server on port 4000
EXPOSE 4000

RUN pip install \
    dagster==${DAGSTER_VERSION} \
    dagster-postgres==${DAGSTER_LIBS_VERSION} \
    dagster-docker==${DAGSTER_LIBS_VERSION} \
    pandas \
    matplotlib \
    numpy \
    girder-client

# Add repository code
COPY . /app
WORKDIR /app
RUN pip install .

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "dagster_htmdec"]
