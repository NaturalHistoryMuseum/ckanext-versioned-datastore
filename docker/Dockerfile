FROM naturalhistorymuseum/ckantest:latest

WORKDIR /base/src/ckanext-versioned-datastore

# copy over the source
COPY . .

# install the base + test dependencies
RUN pip install -e .[test,doi,attribution]

# this entrypoint ensures our service dependencies (postgresql, solr, redis, elasticsearch, and
# mongodb) are running before running the cmd
ENTRYPOINT ["/bin/bash", "/opt/waits/all.sh"]

# run the tests with coverage output
CMD ["bash", "/opt/scripts/run-tests.sh", "ckanext.versioned_datastore"]
