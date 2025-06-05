import hashlib
import secrets
import string
from contextlib import closing
from pathlib import Path
from typing import Iterable

import requests
from ckan.plugins import toolkit
from splitgill.model import Record

from ckanext.versioned_datastore.model.stats import ImportStats


def download_resource_data(resource: dict, into: Path, api_key: str) -> str:
    """
    Downloads the file specified in the resource's url field to the given path and
    returns the SHA1 hash of it. If the url is an upload url (i.e. the URL of a file
    which is stored on this CKAN instance) then the API key will be used to ensure we
    have access. This allows private datasets to have resources ingested in the
    datastore before they are made public.

    :param resource: the resource dict
    :param into: the path to the file where the data should be put
    :param api_key: the user's API key
    :returns: the hash of the downloaded file
    """
    hasher = hashlib.sha1()
    # grab the resource's data via this URL
    url = toolkit.url_for(
        'resource.download',
        id=resource['package_id'],
        resource_id=resource['id'],
        qualified=True,
    )
    # include the auth header regardless of whether the resource URL is of a file hosted
    # by this CKAN instance, or another website. We need to do this as the
    # resource.download route is protected by auth. This is safe and won't leak creds
    # because if the resource's URL is another website, the resource.download route will
    # respond with a redirect to the other site and requests won't copy those headers on
    # to the request to the redirected URL
    headers = {'Authorization': api_key}
    with closing(requests.get(url, stream=True, headers=headers)) as r:
        r.raise_for_status()
        with into.open('wb') as f:
            for chunk in r.iter_content(chunk_size=8192, decode_unicode=False):
                if chunk:
                    f.write(chunk)
                    hasher.update(chunk)

    return hasher.hexdigest()


def iter_records(data: Iterable[dict], stats: ImportStats) -> Iterable[Record]:
    """
    Iterate over the dicts in the given data iterable, converting each to a Record
    object for Splitgill to ingest. The stats object will be updated periodically during
    the operation to show progress (by updating the count value).

    For each dict in the data stream, the _id key is checked to see if it exists. If it
    does exist, the associated value is used as the record ID for the record created
    from that dict. If it does not exist, a new _id value is added to the dict and used
    as the new record's ID.

    New record IDs are generated sequentially to maintain insertion order of these
    records within this data stream. Unless there are more than 1 billion records in the
    stream, the resulting generated IDs will always be 12 characters long. If there are
    more than 1 billion records in the stream, the resulting generated IDs *may* be
    longer than 12 characters.

    IDs take the form of a 3-letter prefix concatenated with the sum of a constant value
    and the record's index in the stream (i.e. the first record is at position 0, the
    10th record is at position 9 etc) in hex. The hex representation is padded with 0s
    to ensure it is at least 9 characters long, hence achieving a 12 character total ID
    length. The constant value is a random number between 0 (inclusive) and 3294967296
    (exclusive) which is chosen because it is 1 billion less than the maximum integer
    expressible in 9 hex characters. This is where the 1 billion soft-limit on IDs of
    length 12 comes from. If the constant is chosen as 3294967295 and a billion records
    IDs are generated, the hex representation of the constant + the last index in the
    stream will be 10 characters long, resulting in a 13 character ID. Is this
    overcomplicated? Perhaps.

    :param data: an iterable of dicts representing record data
    :param stats: the current ImportStats object in use which will be updated with
        progress every 5000 records handled
    :returns: yields Record objects
    """
    stats.update(count=0)

    # choose a random 3 letter prefix for all records in this stream
    prefix = ''.join(secrets.choice(string.ascii_lowercase) for _ in range(3))
    constant = secrets.randbelow(3294967296)
    index = 1

    for record_data in data:
        if index % 5000 == 0:
            stats.update(count=index)

        record_id = record_data.get('_id')
        if not record_id:
            # generate a new id for the record
            record_id = f'{prefix}{constant + index:09x}'
        else:
            # make sure it's a str
            record_id = str(record_id)

        # put the record ID in the record's data, this is necessary if the _id wasn't
        # already in there, and it ensures it is a str if it was
        record_data['_id'] = record_id
        yield Record(record_id, record_data)
        index += 1

    stats.update(count=index)
