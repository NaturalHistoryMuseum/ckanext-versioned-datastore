FROM naturalhistorymuseum/ckantest:0.1

WORKDIR /base/src/ckanext-versioned-datastore

# copy over the ckanext-versioned-datastore source
COPY . .

# install the base + test dependencies
RUN pip install -e .[test]

# this entrypoint ensures our service dependencies (postgresql, solr and redis) are running before
# running the cmd
ENTRYPOINT ["/bin/bash", "/opt/waits/all.sh"]

# run the tests with coverage output
CMD ["pytest", "--cov=ckanext.versioned_datastore", "--ckan-ini=test.ini", "tests"]
