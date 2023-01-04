from .urls import SchemaUrl
from .utils import load_schema


class Extension(object):
    def __init__(self, location, prop_names, row_type, name, core=False):
        self.location = location
        self.prop_names = prop_names or []
        self.row_type = row_type
        self.name = name
        self.core = core

    @classmethod
    def create(cls, url: SchemaUrl, extension_schema=None, core=False):
        if extension_schema is None:
            extension_schema = load_schema(url.url)
        row_type = extension_schema.attrib['rowType']
        name = extension_schema.attrib['name']
        schema_props = extension_schema.findall('.//{*}property')
        prop_names = [p.attrib['name'] for p in schema_props]
        return cls(url, prop_names, row_type, name, core)

    @classmethod
    def load(cls, serialised):
        serialised['location'] = SchemaUrl(**serialised['location'])
        return cls(**serialised)

    def serialise(self):
        return {
            'location': self.location.__dict__,
            'prop_names': self.prop_names,
            'row_type': self.row_type,
            'name': self.name,
            'core': self.core,
        }


class Prop(object):
    def __init__(self, **kwargs):
        # preferable to instantiate with .create() class method instead
        self.name = kwargs.get('name')
        self.iri = kwargs.get('iri')
        self.prop_type = kwargs.get('prop_type', str)
        self.is_identifier = kwargs.get('is_identifier', False)
        self.domain = kwargs.get('domain')
        self.vocabulary = kwargs.get('vocabulary', [])
        self._flags = kwargs.get('flags', [])
        self.extension = kwargs.get('extension')

    @classmethod
    def create(
        cls, row_source=None, xml_element_source=None, flags=None, extension=None
    ):
        flags = flags or []
        new_prop = cls()
        new_prop.update(row_source, xml_element_source, flags, extension)
        return new_prop

    def update(
        self, row_source=None, xml_element_source=None, flags=None, extension=None
    ):
        flags = flags or []
        if row_source is None and xml_element_source is None:
            raise Exception('At least one source required.')
        if row_source is not None:
            self.name = row_source.term_localName
            self.iri = row_source.term_iri
            self.domain = row_source.organized_in
        else:
            self.name = xml_element_source.attrib['name']
            self.iri = xml_element_source.attrib.get('qualName')
            self.domain = xml_element_source.attrib.get('group')
        if xml_element_source is None:
            self.is_identifier = False
            thesaurus_url = None
        else:
            self.is_identifier = (
                xml_element_source.attrib.get('substitutionGroup')
                == 'dwc:anyIdentifier'
            )
            prop_type = xml_element_source.attrib.get('type')
            if prop_type != 'xs:string':
                self.prop_type = prop_type
            thesaurus_url = xml_element_source.attrib.get('thesaurus')
        if thesaurus_url is not None:
            thesaurus = load_schema(thesaurus_url)
            terms = thesaurus.findall('.//{*}concept')
            identifier_key = next(
                k for k in terms[0].attrib.keys() if 'identifier' in k
            )
            self.vocabulary = [t.attrib[identifier_key] for t in terms]
        else:
            self.vocabulary = []
        self._flags = [
            f.lower() for f in set(self._flags + flags) if isinstance(f, str)
        ]
        self.extension = extension

    @property
    def flags(self):
        return [f.lower() for f in self._flags]

    @property
    def core(self):
        return self.extension is None

    def serialise(self):
        return {
            'name': self.name,
            'iri': self.iri,
            'is_identifier': self.is_identifier,
            'domain': self.domain,
            'vocabulary': self.vocabulary,
            'flags': self.flags,
        }

    @classmethod
    def load(cls, serialised):
        return cls(**serialised)

    def __repr__(self):
        if self.vocabulary:
            return f'{self.name} ({len(self.vocabulary)} terms)'
        else:
            return self.name

    def has_flags(self, *flags):
        return all([f.lower() in self.flags for f in flags])


class PropCollection(dict):
    def __init__(self, props):
        if not isinstance(props, dict):
            try:
                props = {p.name: p for p in props}
            except:
                raise TypeError('Must be dict or iterable.')
        super(PropCollection, self).__init__(**props)

    def flagged(self, *flags):
        return PropCollection([v for v in self.values() if v.has_flags(*flags)])

    def serialise(self):
        return {k: v.serialise() for k, v in self.items()}

    @classmethod
    def load(cls, serialised):
        if isinstance(serialised, list):
            serialised = [Prop.load(s) for s in serialised]
        elif isinstance(serialised, dict):
            serialised = {k: Prop.load(v) for k, v in serialised.items()}
        return cls(serialised)


class Domain(object):
    def __init__(self, name, label, iri):
        self.name = name
        self.label = label
        self.iri = iri

    def props(self, schema):
        if schema is None:
            return []
        return [p for p in schema.props if p.domain == self.iri]

    def identifier(self, schema):
        return next(p for p in self.props(schema) if p.is_identifier)

    @classmethod
    def create(cls, source):
        name = source.term_localName
        label = source.label
        iri = source.term_iri
        return cls(name, label, iri)

    def serialise(self):
        return {'name': self.name, 'label': self.label, 'iri': self.iri}

    @classmethod
    def load(cls, serialised):
        return cls(**serialised)

    def __repr__(self):
        return self.name
