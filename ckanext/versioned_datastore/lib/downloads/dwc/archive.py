import csv
import json
import os
import shutil
from collections import OrderedDict
from datetime import datetime as dt

from ckan.plugins import plugin_loaded, toolkit
from lxml import etree

from .schema import Schema
from .urls import TDWGUrls, XMLUrls, GBIFUrls
from .utils import NSMap, json_to_xml, get_setting


class Archive(object):
    def __init__(self, schema: Schema, request, target_dir, output_name=None):
        """
        Produces one output file. Each archive contains a core .csv data file, one .csv
        file for each specified extension, a metafile (meta.xml), and a resource
        metadata file (eml.xml).

        :param schema: a Schema object representing the output data structure
        :param request: the request object
        :param target_dir: the root folder to save files into
        :param output_name: only required if resources are to be separated into different files;
                            usually this is the resource id
        """
        self.schema = schema
        self.dir = target_dir
        self.request = request
        if request.separate_files and not output_name:
            raise Exception(
                'output_name must be specified if separate files are required.'
            )
        self.output_name = output_name or 'dwca'
        self._core_file = None
        self._core_writer = None
        self._ext_files = None
        self._ext_writers = None
        self.rows_written = 0

    @property
    def _uid(self):
        """
        A unique identifier for this particular archive.
        """
        if self.single_resource:
            return self.request.resource_ids[0]
        elif self.request.separate_files:
            return self.output_name
        else:
            return self.request.query_hash

    @property
    def _build_dir(self):
        return os.path.join(self.dir, self._uid)

    @property
    def _core_file_name(self):
        return f'{self.schema.row_type_name.lower()}.csv'

    @property
    def single_resource(self):
        """
        If this download will only include data from a single resource with no filters,
        i.e. just the whole resource.
        """
        return len(self.request.resource_ids) == 1 and self.request.query == {}

    def open(self, field_names):
        """
        Create the build dir, open the necessary files, and create csv writers. Does.

        *not* write the csv headers; this is done in self.initialise().

        :param field_names: field names available in the data rows
        :return: self
        """
        os.mkdir(self._build_dir)
        root_field_names = set([f.split('.')[0] for f in field_names])
        all_ext_field_names = [
            item for ext in self.schema.extensions for item in ext.location.fields
        ]

        standard_fields = ['datasetID', 'basisOfRecord', 'dynamicProperties']
        dataset_fields = [
            f
            for f in root_field_names
            if f not in all_ext_field_names and f in self.schema.props
        ]
        core_field_names = ['_id'] + list(set(standard_fields + dataset_fields))
        self._core_file = open(os.path.join(self._build_dir, self._core_file_name), 'w')
        self._core_writer = csv.DictWriter(
            self._core_file, core_field_names, dialect='unix'
        )

        self._ext_files = {}
        self._ext_writers = {}
        for e in self.schema.extensions:
            subfields = [
                f.split('.')[-1]
                for f in field_names
                if f.split('.')[0] in e.location.fields
            ]
            potential_fields = self.schema.extension_props[e.name]
            ext_field_names = ['_id'] + list(
                set([f for f in subfields if f in potential_fields])
            )
            open_file = open(
                os.path.join(self._build_dir, f'{e.name.lower()}.csv'), 'w'
            )
            self._ext_files[e.name] = open_file
            writer = csv.DictWriter(open_file, ext_field_names, dialect='unix')
            self._ext_writers[e.name] = writer
        return self

    def close(self):
        """
        Close any open files, build the metadata files, and zip everything up.
        """
        if self._core_file is None:
            # this might be called several times due to the defaultdict implementation in writer.py
            return
        self._core_file.close()
        for f in self._ext_files.values():
            f.close()
        with open(os.path.join(self._build_dir, 'meta.xml'), 'w') as f:
            meta_content = self.make_meta()
            f.write(meta_content)
        with open(os.path.join(self._build_dir, 'eml.xml'), 'w') as f:
            eml_content = self.make_eml()
            f.write(eml_content)
        shutil.make_archive(
            os.path.join(self.dir, self.output_name), 'zip', self._build_dir
        )
        shutil.rmtree(self._build_dir)
        self._core_file = None
        self._core_writer = None
        self._ext_files = None
        self._ext_writers = None

    def initialise(self, record):
        """
        Only runs for the first row in the dataset. Checks that certain fields are the
        correct data type before writing the headers.

        :param record: a single row
        """
        if self.rows_written > 0:
            return
        # check 'type' is right
        valid_types = [
            'StillImage',
            'MovingImage',
            'Sound',
            'PhysicalObject',
            'Event',
            'Text',
        ]
        self._core_writer.fieldnames = sorted(self._core_writer.fieldnames)
        if 'type' in record and record['type'] not in valid_types:
            self._core_writer.fieldnames = [
                f for f in self._core_writer.fieldnames if f != 'type'
            ]
        self._core_writer.writeheader()
        for writer in self._ext_writers.values():
            writer.writeheader()

    def _extract_record(self, record, id_field):
        """
        Transform the data in a single row into the required format. Separates extension
        fields from core fields and filters out any fields that don't match the schema.

        :param record: the row of data
        :param id_field: the name of the current id field
        :return: core row (dict), extension rows keyed on extension name (dict of lists of dicts)
        """
        core = {}
        ext = {}
        dynamic_properties = {}

        if id_field not in record:
            raise Exception(f'Record does not have ID field {id_field}')
        record_id = record.get(id_field)
        core['_id'] = record_id

        extension_map = {}
        for e in self.schema.extensions:
            for f in e.location.fields:
                extension_map[f] = e.name

        for k, v in record.items():
            if k in extension_map:
                ext_props = self._ext_writers[extension_map[k]].fieldnames

                def _extract_ext(subdict):
                    props = {'_id': record_id}
                    for ek, ev in subdict.items():
                        if ek in ext_props:
                            props[ek] = ev
                    return props

                if isinstance(v, list):
                    ext_extracted = [_extract_ext(x) for x in v]
                elif isinstance(v, dict):
                    ext_extracted = [_extract_ext(v)]
                else:
                    ext_extracted = [_extract_ext({k: v})]
                ext[extension_map[k]] = ext_extracted
            else:
                if k in self._core_writer.fieldnames:
                    core[k] = v
                else:
                    dynamic_properties[k] = v

        core['dynamicProperties'] = json.dumps(dynamic_properties)
        return core, ext

    def write_record(self, record, id_field='_id'):
        """
        Transform the record then write it to csv.

        :param record: the row of data
        :param id_field: the name of the current id field
        """
        if self._core_file is None:
            # if this is None, the file is not open, and this is being called from the wrong place.
            raise Exception('File is not open.')
        core_row, ext_rows = self._extract_record(record, id_field)
        self._core_writer.writerow(core_row)
        self.rows_written += 1
        for e, rows in ext_rows.items():
            for row in rows:
                self._ext_writers[e].writerow(row)

    def make_meta(self):
        """
        Create the xml text content of the metafile.

        :return: xml string
        """
        nsmap = NSMap(xsi=XMLUrls.xsi, xs=XMLUrls.xs)
        root = etree.Element(
            'archive',
            nsmap=nsmap,
            attrib={
                'xmlns': TDWGUrls.xmlns,
                'metadata': 'eml.xml',
                nsmap.ns('xsi', 'schemaLocation'): ' '.join(
                    [TDWGUrls.xmlns, TDWGUrls.metadata]
                ),
            },
        )
        attributes = {
            'encoding': 'UTF-8',
            'linesTerminatedBy': '\n',
            'fieldsTerminatedBy': ',',
            'fieldsEnclosedBy': '"',
            'ignoreHeaderLines': '1',
        }
        core = etree.SubElement(
            root, 'core', rowType=self.schema.row_type, **attributes
        )
        core_files = etree.SubElement(core, 'files')
        core_files_location = etree.SubElement(core_files, 'location')
        core_files_location.text = self._core_file_name
        etree.SubElement(core, 'id', index='0')
        for i, c in enumerate(self._core_writer.fieldnames):
            if c == '_id':
                continue
            etree.SubElement(
                core, 'field', index=str(i), term=self.schema.props.get(c).iri
            )
        for e in self.schema.extensions:
            ext_root = etree.SubElement(
                root, 'extension', rowType=e.row_type, **attributes
            )
            ext_files = etree.SubElement(ext_root, 'files')
            ext_files_location = etree.SubElement(ext_files, 'location')
            ext_files_location.text = f'{e.name.lower()}.csv'
            etree.SubElement(ext_root, 'coreid', index='0')
            ext_props = self.schema.extension_props[e.name]
            for i, c in enumerate(self._ext_writers[e.name].fieldnames):
                if c == '_id':
                    continue
                etree.SubElement(
                    ext_root, 'field', index=str(i), term=ext_props.get(c).iri
                )
        return etree.tostring(root, pretty_print=True).decode()

    def make_eml(self):
        """
        Create the xml text content of the resource metadata file.

        Tries to use some sensible
        defaults and get information from other relevant plugins where available, but there's still
        the potential for errors or silly data.
        :return: xml string
        """
        # load some useful actions so we don't have to fetch them repeatedly
        resource_show = toolkit.get_action('resource_show')
        package_show = toolkit.get_action('package_show')

        # get the resources and packages associated with the request
        resources = [resource_show({}, {'id': r}) for r in self.request.resource_ids]
        packages = [package_show({}, {'id': r['package_id']}) for r in resources]

        # define some variables
        org = {
            'organizationName': get_setting(
                'ckanext.versioned_datastore.dwc_org_name',
                'ckanext.doi.publisher',
                'ckan.site_title',
            ),
            'electronicMailAddress': get_setting(
                'ckanext.versioned_datastore.dwc_org_email', 'smtp.mail_from'
            ),
            'onlineUrl': get_setting('ckan.site_url'),
        }
        site_name = get_setting('ckanext.doi.site_title', 'ckan.site_title')
        site_url = get_setting('ckan.site_url', default='')
        licenses = list(
            set(
                [
                    p.get('license_title')
                    for p in packages
                    if p.get('license_title') is not None
                ]
            )
        )
        query_license = (
            licenses[0]
            if len(licenses) == 1
            else get_setting(
                'ckanext.versioned_datastore.dwc_default_license', default='null'
            )
        )

        # set up the metadata dict in order (otherwise GBIF complains) with some defaults
        dataset_metadata = OrderedDict(
            {
                'alternateIdentifier': [self.request.query_hash],
                'title': f'Query on {site_name}',
                'creator': [org],
                'metadataProvider': [org],
                'pubDate': dt.now().strftime('%Y-%m-%d'),
                'language': get_setting('ckan.locale_default'),
                'abstract': {
                    'para': f'Query ID {self.request.query_hash} on {site_name} ({self.rows_written} records).'
                },
                'keywordSet': [],
                'intellectualRights': {'para': query_license},
                'distribution': ({'online': {'url': site_url}}, {'scope': 'document'}),
                'contact': [org],
            }
        )
        additional_metadata = {
            'metadata': {
                'gbif': {
                    'dateStamp': dt.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'citation': None,
                    'resourceLogoUrl': site_url
                    + toolkit.config.get('ckan.site_logo', ''),
                }
            }
        }

        if self.single_resource:
            # if it's just downloading one resource with no filters
            resource = resources[0]
            package = packages[0]
            dataset_metadata['title'] = resource['name']
            dataset_metadata['alternateIdentifier'] += self.request.resource_ids
            dataset_metadata['pubDate'] = package.get(
                'metadata_modified', resource.get('created')
            )
            dataset_metadata['abstract'] = {'para': resource.get('description')}
            dataset_metadata['distribution'][0]['online']['url'] = (
                site_url
                + toolkit.url_for(
                    'resource.read', id=package['id'], resource_id=resource['id']
                ),
                {'function': 'information'},
            )
            if 'doi' in package:
                dataset_metadata['alternateIdentifier'].append('doi:' + package['doi'])
                pkg_year = toolkit.h.package_get_year(package)
                pkg_title = package['title']
                pkg_doi = package['doi']
                pkg_author = package['author']
                site_title = toolkit.h.get_site_title()
                additional_metadata['metadata']['gbif']['citation'] = (
                    f'{pkg_author} ({pkg_year}). Dataset: {pkg_title}. {site_title}',
                    {'identifier': f'https://doi.org/{pkg_doi}'},
                )
        else:
            if plugin_loaded('query_dois'):
                from ckanext.query_dois.lib.doi import find_existing_doi

                query_doi = find_existing_doi(
                    self.request.resource_ids_and_versions,
                    self.request.query_hash,
                    self.request.query_version,
                )
                if query_doi:
                    dataset_metadata['alternateIdentifier'] += ['doi:' + query_doi.doi]
                    dataset_metadata['distribution'][0]['online']['url'] = (
                        site_url
                        + toolkit.url_for(
                            'query_doi.landing_page',
                            data_centre=get_setting('ckanext.query_dois.prefix'),
                            identifier=query_doi.doi,
                        ),
                        {'function': 'information'},
                    )
                    additional_metadata['metadata']['gbif']['citation'] = (
                        toolkit.h.create_multisearch_citation_text(
                            query_doi, html=False
                        ),
                        {'identifier': f'https://doi.org/{query_doi.doi}'},
                    )

        if (
            self.schema.core_extension
            and self.schema.core_extension.location.base == GBIFUrls.base_url
        ):
            dataset_metadata['keywordSet'].append(
                {
                    'keyword': self.schema.row_type_name,
                    'keywordThesaurus': f'GBIF Dataset Type Vocabulary: {GBIFUrls.thesaurus}',
                }
            )

        if plugin_loaded('attribution'):
            package_contributions_show = toolkit.get_action(
                'package_contributions_show'
            )
            authors = []
            for r in resources:
                contributions = package_contributions_show({}, {'id': r['package_id']})
                for c in contributions['contributions']:
                    if c['agent']['id'] in authors:
                        continue
                    agent = {}
                    if c['agent']['external_id']:
                        agent['userId'] = (
                            c['agent']['external_id'],
                            {'directory': c['agent']['external_id_url']},
                        )
                    if c['agent']['agent_type'] == 'person':
                        agent['individualName'] = {
                            'givenName': c['agent']['given_names'],
                            'surName': c['agent']['family_name'],
                        }
                    else:
                        agent['organizationName'] = c['agent']['name']
                    dataset_metadata['creator'].append(agent)
                    authors.append(c['agent']['id'])

        nsmap = NSMap(eml=XMLUrls.eml, dc=XMLUrls.dc, xsi=XMLUrls.xsi, xml=XMLUrls.xml)

        root = etree.Element(
            nsmap.ns('eml', 'eml'),
            nsmap=nsmap,
            attrib={
                'scope': 'system',
                'system': 'http://gbif.org',
                nsmap.ns('xsi', 'schemaLocation'): ' '.join(
                    [XMLUrls.eml, GBIFUrls.eml]
                ),
                'packageId': self._uid,
                nsmap.ns('xml', 'lang'): 'en',
            },
        )

        for i in json_to_xml('dataset', dataset_metadata):
            root.append(i)

        if additional_metadata['metadata']['gbif'].get('citation') is not None:
            for i in json_to_xml('additionalMetadata', additional_metadata):
                root.append(i)

        return etree.tostring(root, pretty_print=True).decode()
