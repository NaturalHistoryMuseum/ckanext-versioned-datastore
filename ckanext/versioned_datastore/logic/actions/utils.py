import copy
import inspect
from collections import OrderedDict
from datetime import datetime

from ckan.plugins import toolkit


def validate(context, data_dict, default_schema):
    '''
    Validate the data_dict against a schema. If a schema is not available in the context (under the
    key 'schema') then the default schema is used.

    If the data_dict fails the validation process a ValidationError is raised, otherwise the
    potentially updated data_dict is returned.

    :param context: the ckan context dict
    :param data_dict: the dict to validate
    :param default_schema: the default schema to use if the context doesn't have one
    '''
    schema = context.get(u'schema', default_schema)
    data_dict, errors = toolkit.navl_validate(data_dict, schema, context)
    if errors:
        raise toolkit.ValidationError(errors)
    return data_dict


def action(schema, help, *decorators):
    '''
    Decorator that indicates that the function being decorated is an action function. By wrapping
    a function with this decorator and then passing the module to the create_actions function in
    this module, the developer gains the benefits of:

        - automatic validation against the given schema
        - automatic access check whenever action is called
        - attachment of the given help text to the action function
        - decoration of the action with the given decorators

    :param schema: the schema dict to validate the data_dict's passed to this action against
    :param help: the help text to associate with the action when it is presented to action API users
    :param decorators: a list of decorators to apply to the resulting action function passed to CKAN
    :return: a wrapper function
    '''
    def wrapper(function):
        function.is_action = True
        function.action_schema = schema
        function.action_help = help
        function.action_decorators = decorators
        return function
    return wrapper


def is_action(function):
    '''
    Determines whether the given function is an action or not. This is simply based on the existance
    of the is_action attribute which is set in the action decorator above.

    :param function: the function to check
    :return: True if the function is an action function, False if not
    '''
    return getattr(function, u'is_action', False)


def wrap_action_function(action_name, function):
    '''
    Wrap an action function with useful processes and return it. An action function is a function
    with the action decorator (see the action function decorator in this module). Primarily, this
    allows the following:

        - passing values from the data_dict as proper function parameters thus turning code like
          the following:

            def action(context, data_dict):
                some_param = data_dict.get('some_param', 382)

          into:

            def action(some_param=382):
                ...

          Values are pulled from the data_dict and defaults are used if given in the action function
          definition.
        - Injection of the `context`, `data_dict` and `original_data_dict` variables through their
          inclusion in the action function definition. The original_data_dict is a copy of the
          dict_dict before it is passed to the validate function and therefore provides direct
          access to exactly what was passed when the action was called. To specify these parameters
          you must include them as args, not kwargs.
        - automatic validation using the schema provided with the action function
        - automatic access check
        - attachment of doc which lives separately to the action funciton, this keeps the doc for
          end users and the doc for other developers separate (as the doc exists in the code for the
          actual action function but is then replaced with the provided help text when passed to
          CKAN).

    :param action_name: the name of the action
    :param function: the action function itself that we will be wrapping
    :return: the wrapped action function
    '''
    arg_spec = inspect.getargspec(function)
    if arg_spec.defaults is not None:
        # the default list is used to determine which args are required and which aren't
        required_args = arg_spec.args[:-len(arg_spec.defaults)]
        # create a dict of optional args -> their default values
        optional_args = dict(zip(reversed(arg_spec.args), reversed(arg_spec.defaults)))
    else:
        required_args = arg_spec.args
        optional_args = {}

    # use the action function definition to determine which variables the developer wants injected
    to_inject = []
    for param in {'context', 'data_dict', 'original_data_dict'}:
        if param in required_args:
            # make sure the param is removed from the required args otherwise when the action is run
            # we'll attempt to access it from the data_dict...
            required_args.remove(param)
            to_inject.append(param)

    def action_function(context, data_dict):
        original_data_dict = copy.deepcopy(data_dict)
        data_dict = validate(context, data_dict, function.action_schema)
        toolkit.check_access(action_name, context, data_dict)

        params = {}
        for param_name in to_inject:
            # to avoid having an festival of ifs, use locals()!
            params[param_name] = locals()[param_name]
        for arg in required_args:
            params[arg] = data_dict[arg]
        for arg, default_value in optional_args.items():
            params[arg] = data_dict.get(arg, default_value)
        return function(**params)

    # add the help as the doc so that CKAN finds it and uses it as the help text
    action_function.__doc__ = function.action_help.strip()
    # apply the decorators to the action function we've created
    for action_decorator in function.action_decorators:
        action_function = action_decorator(action_function)

    return action_function


def create_actions(*modules):
    '''
    Finds action functions in the given modules and returns an action dict (action name -> action
    function). Actions are found by finding all functions in each module that meet the is_action
    function criteria (see the is_action function in this module).

    :param modules: the modules to search through
    :return: an actions dict
    '''
    actions = {}

    for module in modules:
        # actions must be functions and pass the is_action function's tests
        functions = inspect.getmembers(module, lambda f: inspect.isfunction(f) and is_action(f))
        for function_name, function in functions:
            actions[function_name] = wrap_action_function(function_name, function)

    return actions


class Timer(object):
    '''
    A simple class which can be used to time events.
    '''

    def __init__(self):
        '''
        The timer is started upon instantiation (i.e. when this function is called).
        '''
        self.start = datetime.now()
        self.events = []

    def add_event(self, label):
        '''
        Add a new event at the current time with the given label.

        :param label: the label for the event
        '''
        self.events.append((label, datetime.now()))

    def to_dict(self):
        '''
        Return an OrderedDict of timings. Each key in the returned OrderedDict is a label and the
        value associated with it is the number of seconds it took between the previous event and
        this one.

        :return: an OrderedDict of events and how long they took
        '''
        timings = OrderedDict()
        split = self.start
        for label, date in self.events:
            timings[label] = (date - split).total_seconds()
            split = date
        return timings
