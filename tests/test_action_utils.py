from ckanext.versioned_datastore.logic.actions.utils import validate
from mock import patch, MagicMock, call


class TestActionUtils(object):
    validate_function = 'ckanext.versioned_datastore.logic.actions.utils.toolkit.navl_validate'

    def test_validate_uses_context_schema(self):
        # mock the validate function to return a mock and False for errors
        mock_validate = MagicMock(return_value=(MagicMock(), False))
        with patch(self.validate_function, side_effect=mock_validate):
            context_schema = MagicMock()
            default_schema = MagicMock()
            data_dict = MagicMock()
            # create a context with a schema specified
            context = {
                'schema': context_schema
            }
            # call validate
            validate(context, data_dict, default_schema)

            # check that the validate function was called with the context schema not the default
            # one
            assert mock_validate.call_args == call(data_dict, context_schema, context)

    def test_validate_uses_default_schema(self):
        mock_validate = MagicMock(return_value=(MagicMock(), False))
        with patch(self.validate_function, side_effect=mock_validate):
            default_schema = MagicMock()
            data_dict = MagicMock()
            # create a context with a no schema specified
            context = {}
            # call validate
            validate(context, data_dict, default_schema)

            # check that the validate function was called with the context schema not the default
            # one
            assert mock_validate.call_args == call(data_dict, default_schema, context)

    def test_validate_returns_validated_data_dict(self):
        # the validation can alter the data dict so we need to ensure that the validate function
        # returns
        # the data_dict passed back from `validate` not the one it was given as an argument
        returned_data_dict = MagicMock()
        # mock the validate function to return the data dict and False for errors
        mock_validate = MagicMock(return_value=(returned_data_dict, False))
        with patch(self.validate_function, side_effect=mock_validate):
            passed_data_dict = MagicMock()
            # check that validate returns the data dict we mock returned from the validate function
            # above not the other MagicMock we passed to it
            data_dict = validate({}, passed_data_dict, MagicMock())
            assert data_dict == returned_data_dict
            assert data_dict != passed_data_dict
