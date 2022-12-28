FROM naturalhistorymuseum/ckantest:0.2

WORKDIR /base/src/ckanext-versioned-datastore

# copy over the source
COPY . .

# install the base + test dependencies
RUN pip install -e .[test]

# this entrypoint ensures our service dependencies (postgresql, solr, redis, elasticsearch, and
# mongodb) are running before running the cmd
ENTRYPOINT ["/bin/bash", "/opt/waits/all.sh"]

# run the tests with coverage output
CMD ["source", "/opt/scripts/run-tests.sh", "ckanext.versioned_datastore"]
