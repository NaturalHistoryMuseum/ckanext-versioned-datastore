from bisect import bisect_right
from typing import Optional

from splitgill.indexing.options import ParsingOptionsBuilder
from splitgill.model import ParsingOptions

from ckanext.versioned_datastore.lib.utils import get_database, ivds_implementations


def get_options(
    resource_id: str,
    version: Optional[int] = None,
) -> Optional[ParsingOptions]:
    """
    Retrieves the resource's parsing options that apply for the given version. If the
    version is None, returns the latest options.

    :param resource_id: the resource ID
    :param version: optional version which, if passed, will mean the options returned
        are the options that would be available at the given version.
    :returns: a ParsingOptions instance or None if no parsing options are in use
    """
    database = get_database(resource_id)
    all_options = database.get_options(include_uncommitted=False)
    if not all_options:
        return None

    versions = sorted(all_options.keys())
    if version is None:
        # no version provided, return the latest options
        return all_options[versions[-1]]
    else:
        # guard against the version requested being below the earliest options version
        if version < versions[0]:
            return None

        # figure out which options version is in use at the requested version
        rounded_version = versions[bisect_right(versions, version) - 1]
        return all_options[rounded_version]


def update_options(resource_id: str, overrides: Optional[dict] = None) -> Optional[int]:
    """
    Update the options for the given resource using the overrides if given. If no
    options are set on the resource then some default ones are created and added.

    :param resource_id: the resource ID
    :param overrides: any override options to set
    :returns: the new options version that has been committed or None if nothing was
        changed
    """
    overrides = overrides or {}
    current_options = get_options(resource_id)
    if current_options is None:
        builder = create_default_options_builder()
    else:
        builder = ParsingOptionsBuilder(based_on=current_options)

    if 'keyword_length' in overrides:
        builder.with_keyword_length(overrides['keyword_length'])
    if 'float_format' in overrides:
        builder.with_float_format(overrides['float_format'])
    for true_value in overrides.get('true_values', []):
        builder.with_true_value(true_value)
    for false_value in overrides.get('false_values', []):
        builder.with_false_value(false_value)
    for date_format in overrides.get('date_formats', []):
        builder.with_date_format(date_format)
    for geo_hint in overrides.get('geo_hints', []):
        builder.with_geo_hint(*geo_hint)

    for plugin in ivds_implementations():
        plugin.vds_update_options(resource_id, builder)

    database = get_database(resource_id)
    return database.update_options(builder.build(), commit=True)


def create_default_options_builder() -> ParsingOptionsBuilder:
    """
    Creates an options builder with some sensible default options set. This is
    deliberately pretty slim (just the minimum required options are set). Use
    vds_update_options to make modifications.

    :returns: a ParsingOptionsBuilder instance
    """
    builder = ParsingOptionsBuilder()
    builder.with_keyword_length(8191)
    builder.with_float_format('{0:.15g}')
    return builder
