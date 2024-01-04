import csv
import json
import os
import shutil
from collections import OrderedDict
from datetime import datetime as dt
from uuid import uuid4

from lxml import etree

from ckan.plugins import toolkit, plugin_loaded, PluginImplementations
from . import urls, utils
from .schema import Schema
from ..base import BaseDerivativeGenerator
from .....interfaces import IVersionedDatastoreDownloads


class DwcDerivativeGenerator(BaseDerivativeGenerator):
    name = 'dwc'
    extension = 'zip'

    def __init__(
        self,
        output_dir,
        fields,
        query,
        resource_id=None,
        core_extension_name=None,
        extension_names=None,
        extension_map=None,
        id_field='_id',
        eml_title=None,
        eml_abstract=None,
        **format_args,
    ):
        super(DwcDerivativeGenerator, self).__init__(
            output_dir, fields, query, resource_id, **format_args
        )
        self._id_field = id_field
        self._eml_title = eml_title
        self._eml_abstract = eml_abstract
        self._build_dir_name = uuid4().hex
        self._build_dir = os.path.join(self.output_dir, self._build_dir_name)
        os.mkdir(self._build_dir)

        # LOAD THE SCHEMA
        schema_args = {}
        # there can be maximum one core extension
        core_extension_name = core_extension_name or toolkit.config.get(
            'ckanext.versioned_datastore.dwc_core_extension_name'
        )
        if (
            core_extension_name is not None
            and core_extension_name.lower() in urls.core_extensions
        ):
            core_ext_url = urls.core_extensions.get(core_extension_name.lower())
            schema_args['core_extension_url'] = core_ext_url
        # there can be multiple non-core extensions, separated by commas in ckan.ini or supplied as
        # a list in the request args
        if extension_names is None:
            extension_names = [
                e.strip().lower()
                for e in toolkit.config.get(
                    'ckanext.versioned_datastore.dwc_extension_names', ''
                ).split(',')
            ]
        # the fields used by the extension can also be overridden by request args
        extension_map = extension_map or {}
        ext_urls = []
        for e in extension_names:
            ext = urls.extensions.get(e)
            if not ext:
                continue
            fields = extension_map.get(e)
            if fields is not None and isinstance(fields, list):
                ext.fields = fields
            if ext.fields:  # no point in adding it if no fields are defined
                ext_urls.append(ext)
        if len(ext_urls) > 0:
            schema_args['extension_urls'] = ext_urls
        self.schema = Schema.load(
            toolkit.config.get('ckanext.versioned_datastore.dwc_schema_cache'),
            **schema_args,
        )

        # set up file paths & divide fields between core/extensions
        self._core_file_name = f'{self.schema.row_type_name.lower()}.csv'
        self.file_paths = {'core': os.path.join(self._build_dir, self._core_file_name)}
        root_field_names = set([f.split('.')[0] for f in self.all_fields])
        all_ext_field_names = [
            item for ext in self.schema.extensions for item in ext.location.fields
        ]
        dataset_fields = [
            f
            for f in root_field_names
            if f not in all_ext_field_names and f in self.schema.props
        ]
        self.fields = {
            'core': ['_id'] + sorted(list(set(utils.standard_fields + dataset_fields)))
        }

        for e in self.schema.extensions:
            self.file_paths[e.name] = os.path.join(
                self._build_dir, f'{e.name.lower()}.csv'
            )
            subfields = [
                f.split('.')[-1]
                for f in self.all_fields
                if f.split('.')[0] in e.location.fields
            ]
            potential_fields = self.schema.extension_props[e.name]
            ext_field_names = ['_id'] + list(
                set([f for f in subfields if f in potential_fields])
            )
            self.fields[e.name] = ext_field_names
        # this will contain csv writers for each component csv
        self.writers = {}
        # add meta and eml paths
        self.file_paths['_meta'] = os.path.join(self._build_dir, 'meta.xml')
        self.file_paths['_eml'] = os.path.join(self._build_dir, 'eml.xml')

        # counts number of rows written
        self.rows_written = 0

    def __exit__(self, *args, **kwargs):
        super(DwcDerivativeGenerator, self).__exit__(*args, **kwargs)
        shutil.make_archive(
            os.path.join(self.output_dir, os.path.splitext(self.output_name)[0]),
            'zip',
            self._build_dir,
        )

    def setup(self):
        if not self._opened:
            raise Exception('Files should be open.')
        self.writers = {
            k: csv.DictWriter(self.files[k], self.fields[k], dialect='unix')
            for k in self.fields
        }
        # headers are written in .validate()
        super(DwcDerivativeGenerator, self).setup()

    def validate(self, record):
        if 'type' in record and record['type'] not in utils.valid_types:
            self.writers['core'].fieldnames = [
                f for f in self.fields['core'] if f != 'type'
            ]
        for k, writer in self.writers.items():
            writer.writeheader()
        super(DwcDerivativeGenerator, self).validate(record)

    def finalise(self):
        self.files['_meta'].write(self.make_meta())
        self.files['_eml'].write(self.make_eml())
        self.writers = {}

    def cleanup(self):
        try:
            shutil.rmtree(self._build_dir)
        except FileNotFoundError:
            pass

    def _write(self, record):
        core_row, ext_rows = self._extract_record(record)
        self.writers['core'].writerow(core_row)
        for e, rows in ext_rows.items():
            for row in rows:
                self.writers[e].writerow(row)
        self.rows_written += 1

    def _extract_record(self, record):
        """
        Transform the data in a single row into the required format. Separates extension
        fields from core fields and filters out any fields that don't match the schema.

        :param record: the row of data
        :return: core row (dict), extension rows keyed on extension name (dict of lists of dicts)
        """
        core = {}
        ext = {}
        dynamic_properties = {}

        if self._id_field not in record:
            raise Exception(f'Record does not have ID field {self._id_field}')
        record_id = record.get(self._id_field)
        core['_id'] = record_id

        extension_map = {}
        for e in self.schema.extensions:
            for f in e.location.fields:
                extension_map[f] = e.name

        for k, v in record.items():
            if k in extension_map:
                ext_props = self.writers[extension_map[k]].fieldnames

                def _extract_ext(subdict):
                    props = {'_id': record_id}
                    for ek, ev in subdict.items():
                        if ek in ext_props and ek != '_id':
                            props[ek] = ev
                    return props

                if isinstance(v, list):
                    ext_extracted = [_extract_ext(x) for x in v]
                elif isinstance(v, dict):
                    ext_extracted = [_extract_ext(v)]
                elif v is None:
                    # skip if empty
                    ext_extracted = []
                else:
                    ext_extracted = [_extract_ext({k: v})]
                ext[extension_map[k]] = ext_extracted
            else:
                if k in self.writers['core'].fieldnames:
                    core[k] = v
                else:
                    dynamic_properties[k] = v

        core['dynamicProperties'] = json.dumps(dynamic_properties)
        return core, ext

    def make_meta(self):
        """
        Create the xml text content of the metafile.

        :return: xml string
        """
        nsmap = utils.NSMap(xsi=urls.XMLUrls.xsi, xs=urls.XMLUrls.xs)
        root = etree.Element(
            'archive',
            nsmap=nsmap,
            attrib={
                'xmlns': urls.TDWGUrls.xmlns,
                'metadata': 'eml.xml',
                nsmap.ns('xsi', 'schemaLocation'): ' '.join(
                    [urls.TDWGUrls.xmlns, urls.TDWGUrls.metadata]
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
        for i, c in enumerate(self.writers['core'].fieldnames):
            if c == '_id':
                continue
            prop = self.schema.props.get(c)
            if prop is None:
                continue
            etree.SubElement(core, 'field', index=str(i), term=prop.iri)
        for e in self.schema.extensions:
            ext_root = etree.SubElement(
                root, 'extension', rowType=e.row_type, **attributes
            )
            ext_files = etree.SubElement(ext_root, 'files')
            ext_files_location = etree.SubElement(ext_files, 'location')
            ext_files_location.text = f'{e.name.lower()}.csv'
            etree.SubElement(ext_root, 'coreid', index='0')
            ext_props = self.schema.extension_props[e.name]
            for i, c in enumerate(self.writers[e.name].fieldnames):
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

        # get the resources and packages associated with the query
        resources = [
            resource_show({}, {'id': r}) for r in self._query.resource_ids_and_versions
        ]
        packages = [package_show({}, {'id': r['package_id']}) for r in resources]

        # useful bools
        single_resource = len(resources) == 1
        single_package = len(packages) == 1
        empty_query = self._query.query == {}

        # define some site variables
        site_org = {
            'organizationName': utils.get_setting(
                'ckanext.versioned_datastore.dwc_org_name',
                'ckanext.doi.publisher',
                'ckan.site_title',
            ),
            'electronicMailAddress': utils.get_setting(
                'ckanext.versioned_datastore.dwc_org_email', 'smtp.mail_from'
            ),
            'onlineUrl': utils.get_setting('ckan.site_url'),
        }
        site_name = utils.get_setting('ckanext.doi.site_title', 'ckan.site_title')
        site_url = utils.get_setting('ckan.site_url', default='')

        # get the license
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
            else utils.get_setting(
                'ckanext.versioned_datastore.dwc_default_license', default='null'
            )
        )

        # generate a string representing the records' location, e.g. a single resource
        # name, a package name, or the whole site
        if single_resource:
            container_name = resources[0]['name']
        elif single_package:
            container_name = f'{len(resources)} resources in {packages[0]["title"]}'
        else:
            container_name = site_name

        # generate title and abstract
        query_title = self._eml_title or f'Query on {container_name}'
        query_abstract = (
            self._eml_abstract
            or f'Query ID {self._query.hash} on {container_name} ({self.rows_written} records).'
        )

        # set up the metadata dict in order (otherwise GBIF complains) with some defaults
        dataset_metadata = OrderedDict(
            {
                'alternateIdentifier': [self._query.hash],
                'title': query_title,
                'creator': [site_org],
                'metadataProvider': [site_org],
                'pubDate': dt.now().strftime('%Y-%m-%d'),
                'language': utils.get_setting('ckan.locale_default'),
                'abstract': {'para': query_abstract},
                'keywordSet': [],
                'intellectualRights': {'para': query_license},
                'distribution': (
                    {'online': {'url': (site_url, {'function': 'information'})}},
                    {'scope': 'document'},
                ),
                'coverage': {
                    'geographicCoverage': {
                        'geographicDescription': 'Unbound',
                        'boundingCoordinates': {
                            'westBoundingCoordinates': -180,
                            'eastBoundingCoordinates': 180,
                            'northBoundingCoordinates': -90,
                            'southBoundingCoordinates': 90,
                        },
                    }
                },
                'contact': [site_org],
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

        if single_package and empty_query:
            # all the resources are in a single package (even if not all resources in
            # that package are included), so link to the package
            dataset_metadata['distribution'][0]['online']['url'] = (
                toolkit.url_for('package.read', id=packages[0]['id'], qualified=True),
                {'function': 'information'},
            )

            # also link to the package via the doi
            if 'doi' in packages[0]:
                dataset_metadata['alternateIdentifier'].append(
                    'doi:' + packages[0]['doi']
                )
                pkg_year = toolkit.h.package_get_year(packages[0])
                pkg_title = packages[0]['title']
                pkg_doi = packages[0]['doi']
                pkg_author = packages[0]['author']
                additional_metadata['metadata']['gbif']['citation'] = (
                    f'{pkg_author} ({pkg_year}). Dataset: {pkg_title}. {site_name}',
                    {'identifier': f'https://doi.org/{pkg_doi}'},
                )

            if single_resource and empty_query:
                # if it's just downloading one resource with no filters, we can be even
                # more specific with some of the metadata
                resource = resources[0]
                package = packages[0]
                dataset_metadata['title'] = resource['name']
                dataset_metadata['alternateIdentifier'] += [resource['id']]
                dataset_metadata['pubDate'] = package.get(
                    'metadata_modified', resource.get('created')
                )
                dataset_metadata['abstract']['para'] = resource.get('description')
                dataset_metadata['distribution'][0]['online']['url'] = (
                    toolkit.url_for(
                        'resource.read',
                        id=package['id'],
                        resource_id=resource['id'],
                        qualified=True,
                    ),
                    {'function': 'information'},
                )
        elif plugin_loaded('query_dois'):
            from ckanext.query_dois.lib.doi import find_existing_doi

            query_doi = find_existing_doi(
                self._query.resource_ids_and_versions,
                self._query.hash,
                self._query.query_version,
            )
            if query_doi:
                dataset_metadata['alternateIdentifier'] += ['doi:' + query_doi.doi]
                dataset_metadata['distribution'][0]['online']['url'] = (
                    toolkit.url_for(
                        'query_doi.landing_page',
                        data_centre=utils.get_setting('ckanext.query_dois.prefix'),
                        identifier=query_doi.doi.split('/', 1)[-1],
                        qualified=True,
                    ),
                    {'function': 'information'},
                )
                additional_metadata['metadata']['gbif']['citation'] = (
                    toolkit.h.create_multisearch_citation_text(query_doi, html=False),
                    {'identifier': f'https://doi.org/{query_doi.doi}'},
                )

        if (
            self.schema.core_extension
            and self.schema.core_extension.location.base == urls.GBIFUrls.base_url
        ):
            dataset_metadata['keywordSet'].append(
                {
                    'keyword': self.schema.row_type_name,
                    'keywordThesaurus': f'GBIF Dataset Type Vocabulary: {urls.GBIFUrls.thesaurus}',
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

        for plugin in PluginImplementations(IVersionedDatastoreDownloads):
            dataset_metadata = plugin.download_modify_eml(dataset_metadata, self._query)

        nsmap = utils.NSMap(
            eml=urls.XMLUrls.eml,
            dc=urls.XMLUrls.dc,
            xsi=urls.XMLUrls.xsi,
            xml=urls.XMLUrls.xml,
        )

        root = etree.Element(
            nsmap.ns('eml', 'eml'),
            nsmap=nsmap,
            attrib={
                'scope': 'system',
                'system': 'http://gbif.org',
                nsmap.ns('xsi', 'schemaLocation'): ' '.join(
                    [urls.XMLUrls.eml, urls.GBIFUrls.eml]
                ),
                'packageId': self.output_name,
                nsmap.ns('xml', 'lang'): 'en',
            },
        )

        for i in utils.json_to_xml('dataset', dataset_metadata):
            root.append(i)

        if additional_metadata['metadata']['gbif'].get('citation') is not None:
            for i in utils.json_to_xml('additionalMetadata', additional_metadata):
                root.append(i)

        return etree.tostring(root, pretty_print=True).decode()
