from setuptools import setup, find_packages

version = '0.1'

setup(
    name='ckanext-versioned-datastore',
    version=version,
    description="",
    long_description='''
    ''',
    classifiers=[],
    keywords='',
    author=['Josh Humphries'],
    author_email='data@nhm.ac.uk',
    url='',
    license='',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext', 'ckanext.versioned_datastore'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'elasticsearch==6.3.1',
        'elasticsearch-dsl==6.2.1',
        'eevee==0.1',
    ],
    dependency_links=[
        'git+git://github.com/NaturalHistoryMuseum/eevee.git#egg=eevee-0.1',
    ],
    entry_points='''
        [ckan.plugins]
        versioned_datastore=ckanext.versioned_datastore.plugin:VersionedSearchPlugin
        [paste.paster_command]
        initdb=ckanext.versioned_datastore.commands.initdb:VersionedDatastoreInitDBCommand
    ''',
)
