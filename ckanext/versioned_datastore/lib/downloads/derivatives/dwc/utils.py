from io import BytesIO

import requests
from lxml import etree

from ckan.plugins import toolkit

parser = etree.XMLParser(recover=True)

standard_fields = ['datasetID', 'basisOfRecord', 'dynamicProperties']
valid_types = ['StillImage', 'MovingImage', 'Sound', 'PhysicalObject', 'Event', 'Text']


def load_schema(url, base_url=None):
    # lxml does not like https urls, so use requests to get the bytes
    r = requests.get(url)
    content = BytesIO(r.content)
    return etree.parse(content, base_url=base_url).getroot()


def json_to_xml(tag, content):
    if isinstance(content, tuple):
        attrs = content[1]
        content = content[0]
    else:
        attrs = {}

    if isinstance(content, dict):
        node = etree.Element(tag, **attrs)
        for k, v in content.items():
            for n in json_to_xml(k, v):
                node.append(n)
        return [node]
    elif isinstance(content, list):
        nodes = [item for e in content for item in json_to_xml(tag, e)]
        return nodes
    else:
        node = etree.Element(tag, **attrs)
        node.text = str(content)
        return [node]


def get_setting(*config_names, default=None):
    setting = None
    for c in config_names:
        setting = toolkit.config.get(c)
        if setting is not None:
            break
    return setting or default


class NSMap(dict):
    def ns(self, key, tag):
        return f'{{{self[key]}}}{tag}'
