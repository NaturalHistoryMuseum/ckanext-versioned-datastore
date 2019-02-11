from eevee.indexing.utils import DOC_TYPE
from mock import MagicMock, call

from ckanext.versioned_datastore.lib.utils import validate, format_facets, is_datastore_resource, \
    get_fields


def test_validate_uses_context_schema(monkeypatch):
    # mock the validate function to return a mock and False for errors
    mock_validate = MagicMock(return_value=(MagicMock(), False))
    monkeypatch.setattr(u'ckanext.versioned_datastore.lib.utils.dictization_functions',
                        MagicMock(validate=mock_validate))

    context_schema = MagicMock()
    default_schema = MagicMock()
    data_dict = MagicMock()
    # create a context with a schema specified
    context = {u'schema': context_schema}
    # call validate
    validate(context, data_dict, default_schema)

    # check that the validate function was called with the context schema not the default one
    assert mock_validate.call_args == call(data_dict, context_schema, context)


def test_validate_uses_default_schema(monkeypatch):
    # mock the validate function to return a mock and False for errors
    mock_validate = MagicMock(return_value=(MagicMock(), False))
    monkeypatch.setattr(u'ckanext.versioned_datastore.lib.utils.dictization_functions',
                        MagicMock(validate=mock_validate))

    default_schema = MagicMock()
    data_dict = MagicMock()
    # create a context with a no schema specified
    context = {}
    # call validate
    validate(context, data_dict, default_schema)

    # check that the validate function was called with the context schema not the default one
    assert mock_validate.call_args == call(data_dict, default_schema, context)


def test_validate_returns_validated_data_dict(monkeypatch):
    # the validation can alter the data dict so we need to ensure that the validate function returns
    # the data_dict passed back from `validate` not the one it was given as an argument
    returned_data_dict = MagicMock()
    # mock the validate function to return the data dict and False for errors
    mock_validate = MagicMock(return_value=(returned_data_dict, False))
    monkeypatch.setattr(u'ckanext.versioned_datastore.lib.utils.dictization_functions',
                        MagicMock(validate=mock_validate))

    passed_data_dict = MagicMock()
    # check that validate returns the data dict we mock returned from the validate function above,
    # not the other MagicMock we passed to it
    data_dict = validate({}, passed_data_dict, MagicMock())
    assert data_dict == returned_data_dict
    assert data_dict != passed_data_dict


def test_format_facets(monkeypatch):
    # first, check it can deal with an empty aggregation result
    assert format_facets({}) == {}

    facets = format_facets({
        u'facet1': {
            u'sum_other_doc_count': 901,
            u'doc_count_error_upper_bound': 12,
            u'buckets': [
                {u'key': u'value1', u'doc_count': 43},
                {u'key': u'value2', u'doc_count': 243},
                {u'key': u'value3', u'doc_count': 543},
                {u'key': u'value4', u'doc_count': 143},
                {u'key': u'value5', u'doc_count': 743},
            ]
        },
        u'facet2': {
            u'sum_other_doc_count': 0,
            u'doc_count_error_upper_bound': 0,
            u'buckets': [
                {u'key': u'value1', u'doc_count': 6},
            ]
        }
    })

    assert len(facets) == 2
    assert facets[u'facet1'][u'details'][u'sum_other_doc_count'] == 901
    assert facets[u'facet1'][u'details'][u'doc_count_error_upper_bound'] == 12
    assert len(facets[u'facet1'][u'values']) == 5
    assert facets[u'facet1'][u'values'][u'value1'] == 43
    assert facets[u'facet1'][u'values'][u'value2'] == 243
    assert facets[u'facet1'][u'values'][u'value3'] == 543
    assert facets[u'facet1'][u'values'][u'value4'] == 143
    assert facets[u'facet1'][u'values'][u'value5'] == 743

    assert facets[u'facet2'][u'details'][u'sum_other_doc_count'] == 0
    assert facets[u'facet2'][u'details'][u'doc_count_error_upper_bound'] == 0
    assert len(facets[u'facet2'][u'values']) == 1
    assert facets[u'facet2'][u'values'][u'value1'] == 6


def test_get_fields(monkeypatch):
    mock_mapping = {
        u"index_name": {
            u"mappings": {
                DOC_TYPE: {
                    u"properties": {
                        u"data": {
                            u"properties": {
                                u"field1": {
                                    u"type": u"keyword",
                                },
                                u"field2": {
                                    u"type": u"date",
                                },
                            }
                        }
                    }
                }
            }
        }
    }

    mapping_mock_function = MagicMock(return_value=mock_mapping)
    searcher_mock = MagicMock(elasticsearch=MagicMock(indices=MagicMock(get_mapping=
                                                                        mapping_mock_function)))
    monkeypatch.setattr(u'ckanext.versioned_datastore.lib.utils.get_searcher',
                        MagicMock(return_value=searcher_mock))

    mapping, fields = get_fields(u'index')
    assert mapping == mock_mapping
    assert len(fields) == 2
    assert {u'id': u'field1', u'type': u'string'} in fields
    assert {u'id': u'field2', u'type': u'string'} in fields


def test_is_datastore_resource(monkeypatch):
    exists_mock = MagicMock(return_value=True)
    prefix_mock = lambda name: u'beans-{}'.format(name)
    searcher_mock = MagicMock(prefix_index=prefix_mock, elasticsearch=MagicMock(indices=MagicMock(
        exists=exists_mock)))
    monkeypatch.setattr(u'ckanext.versioned_datastore.lib.utils.get_searcher',
                        MagicMock(return_value=searcher_mock))

    assert is_datastore_resource(u'banana')
    assert exists_mock.call_args == call(u'beans-banana')
