from setuptools import setup, find_packages

version = u'0.1'

setup(
    name=u'ckanext-versioned-datastore',
    version=version,
    description=u"",
    long_description=u'''
    ''',
    classifiers=[],
    keywords=u'',
    author=[u'Josh Humphries'],
    author_email=u'data@nhm.ac.uk',
    url=u'',
    license=u'',
    packages=find_packages(exclude=[u'ez_setup', u'examples', u'tests']),
    namespace_packages=[u'ckanext', u'ckanext.versioned_datastore'],
    include_package_data=True,
    zip_safe=False,
    # apparently for CKAN extensions all dependencies should be in the requirements.txt
    # and none should be in here...
    install_requires=[],
    entry_points=u'''
        [ckan.plugins]
        versioned_datastore=ckanext.versioned_datastore.plugin:VersionedSearchPlugin
        [paste.paster_command]
        vds=ckanext.versioned_datastore.commands:VersionedDatastoreCommands
    ''',
)
