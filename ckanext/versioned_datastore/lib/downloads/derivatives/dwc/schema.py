import json
import os

import pandas as pd

from .schema_parts import Domain, Extension, Prop, PropCollection
from .urls import TDWGUrls
from .utils import load_schema


class Schema(object):
    def __init__(
        self,
        domains,
        props,
        core_extension=None,
        extensions=None,
        extension_props=None,
        row_type='http://rs.tdwg.org/dwc/terms/Occurrence',
    ):
        self.domains = domains
        self.props = props
        self.core_extension = core_extension
        self.extensions = extensions or []
        self.extension_props = extension_props or PropCollection([])
        self.row_type = core_extension.row_type if core_extension else row_type
        self.row_type_name = (
            core_extension.name if core_extension else row_type.split('/')[-1]
        )

    @classmethod
    def regenerate(cls, core_extension_url=None, extension_urls=None):
        extension_urls = extension_urls or []

        df = pd.read_csv(TDWGUrls.terms_csv)
        # ignore deprecated terms
        df = df[df.status == 'recommended']
        # drop some columns we don't need
        df = df.drop(['issued', 'status', 'replaces'], axis=1)

        domains_df = df[df.rdf_type == 'http://www.w3.org/2000/01/rdf-schema#Class']
        domains_df = domains_df.drop(['organized_in', 'rdf_type'], axis=1)
        domains = [Domain.create(d) for _, d in domains_df.iterrows()]

        props_df = df[
            df.rdf_type == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#Property'
        ]
        props_df = props_df.drop(['rdf_type'], axis=1)

        # supplementary property information is in the core schema
        core_xsd = load_schema(TDWGUrls.core, base_url=TDWGUrls.base_url)
        core_props = {}
        for _, p in props_df.iterrows():
            xml_element = core_xsd.find(f'.//{{*}}element[@name="{p.term_localName}"]')
            # (hopefully) temporary fix for https://github.com/tdwg/dwc/issues/403
            if xml_element is None and p.term_localName == 'degreeOfEstablishment':
                xml_element = core_xsd.find(
                    './/{*}element[@name="degreeOfEstablishmentMeans"]'
                )
            elif xml_element is None and p.term_localName == 'waterBody':
                xml_element = core_xsd.find('.//{*}element[@name="waterbody"]')
            prop = Prop.create(p, xml_element, flags=[p['flags']])
            core_props[prop.name] = prop

        if core_extension_url is not None:
            temp_props = {}
            extension_schema = load_schema(
                core_extension_url.url, core_extension_url.base
            )
            ce_props = extension_schema.findall('.//{*}property')
            for p in ce_props:
                domain_name = p.attrib['group']
                prop_name = p.attrib['name']
                prop = core_props.get(prop_name)
                if prop is not None:
                    prop.update(xml_element_source=p)
                else:
                    prop = Prop.create(xml_element_source=p, flags=['core_extension'])
                temp_props[prop.name] = prop
            core_props = temp_props
            ce = Extension.create(core_extension_url, extension_schema, core=True)
        else:
            ce = None

        props = PropCollection(core_props.values())

        extensions = []
        extension_props = {}
        for e in extension_urls:
            extension_schema = load_schema(e.url, e.base)
            ext = Extension.create(e, extension_schema)
            extensions.append(ext)
            schema_props = extension_schema.findall('.//{*}property')
            extension_props[ext.name] = PropCollection(
                [
                    Prop.create(xml_element_source=p, extension=ext.name)
                    for p in schema_props
                ]
            )

        return cls(domains, props, ce, extensions, extension_props)

    def serialise(self):
        serialised_dict = {
            'domains': [d.serialise() for d in self.domains],
            'props': self.props.serialise(),
            'row_type': self.row_type,
        }
        if self.core_extension is not None:
            serialised_dict['core_extension'] = self.core_extension.serialise()
        if len(self.extensions) > 0:
            serialised_dict['extensions'] = [e.serialise() for e in self.extensions]
            serialised_dict['extension_props'] = {
                k: v.serialise() for k, v in self.extension_props.items()
            }
        return serialised_dict

    def save(self, cache_path):
        with open(cache_path, 'w') as f:
            json.dump(self.serialise(), f, indent=2)

    @classmethod
    def load(cls, cache_path=None, **kwargs):
        if cache_path is None:
            return cls.regenerate(**kwargs)
        if os.path.exists(cache_path):
            with open(cache_path, 'r') as f:
                content = json.load(f)
            init_dict = {
                'domains': [Domain.load(d) for d in content['domains']],
                'props': PropCollection.load(content['props']),
                'extensions': [
                    Extension.load(e) for e in content.get('extensions', [])
                ],
                'extension_props': {
                    k: PropCollection.load(v)
                    for k, v in content.get('extension_props', {}).items()
                },
                'row_type': content.get(
                    'row_type', 'http://rs.tdwg.org/dwc/terms/Occurrence'
                ),
            }
            ce = content.get('core_extension')
            if ce is not None:
                init_dict['core_extension'] = Extension.load(ce)
                init_dict['row_type'] = init_dict['core_extension'].row_type
            return cls(**init_dict)
        else:
            new_archiver = cls.regenerate(**kwargs)
            new_archiver.save(cache_path)
            return new_archiver
