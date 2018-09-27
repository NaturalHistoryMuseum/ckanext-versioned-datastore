# Versioned Datastore for CKAN

A CKAN extension providing a versioned datastore using MongoDB, Elasticsearch and [Eevee](https://github.com/NaturalHistoryMuseum/eevee).


**This project is currently under active development.**


### Running the tests
The tests for this project are written using pytest. The reasons for this are twofold:

  - it's on CKAN's roadmap to replace nose with pytest
  - it's really nice to use

To run the tests, make sure you have the required modules installed (`pip install ckanext/versioned_datastore/tests/requirements.txt`) and then simply run

```
pytest
```

from the root directory of this project.

If you want to run with coverage too do 

```
pytest --cov=ckanext.versioned_datastore
```
