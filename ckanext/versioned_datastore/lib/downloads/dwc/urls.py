from dataclasses import dataclass


@dataclass
class SchemaUrl:
    url: str
    base: str
    fields: list = None


class TDWGUrls:
    base_url = 'https://dwc.tdwg.org/xml'
    xmlns = 'http://rs.tdwg.org/dwc/text'
    core = 'https://dwc.tdwg.org/xml/tdwg_dwcterms.xsd'
    base_types = 'https://dwc.tdwg.org/xml/tdwg_basetypes.xsd'
    core_classes = 'https://dwc.tdwg.org/xml/tdwg_dwc_classes.xsd'
    core_class_terms = 'https://dwc.tdwg.org/xml/tdwg_dwc_class_terms.xsd'
    simple = 'https://dwc.tdwg.org/xml/tdwg_dwc_simple.xsd'
    terms_csv = (
        'https://raw.githubusercontent.com/tdwg/dwc/master/vocabulary/term_versions.csv'
    )
    metadata = 'http://rs.tdwg.org/dwc/text/tdwg_dwc_text.xsd'


class GBIFUrls:
    base_url = 'https://rs.gbif.org'
    eml = 'http://rs.gbif.org/schema/eml-gbif-profile/1.1/eml.xsd'
    thesaurus = 'http://rs.gbif.org/vocabulary/gbif/dataset_type.xml'


class XMLUrls:
    xsi = 'http://www.w3.org/2001/XMLSchema-instance'
    xs = 'http://www.w3.org/2001/XMLSchema'
    xml = 'http://www.w3.org/XML/1998/namespace'
    dc = 'http://purl.org/dc/terms'
    eml = 'eml://ecoinformatics.org/eml-2.1.1'


core_extensions = {
    'gbif_occurrence': SchemaUrl(
        'https://rs.gbif.org/core/dwc_occurrence_2020-07-15.xml', GBIFUrls.base_url
    ),
    'gbif_taxon': SchemaUrl(
        'https://rs.gbif.org/core/dwc_taxon_2015-04-24.xml', GBIFUrls.base_url
    ),
    'gbif_event': SchemaUrl(
        'https://rs.gbif.org/core/dwc_event_2016_06_21.xml', GBIFUrls.base_url
    ),
}

extensions = {
    'gbif_multimedia': SchemaUrl(
        'https://rs.gbif.org/extension/gbif/1.0/multimedia.xml',
        GBIFUrls.base_url,
        ['associatedMedia'],
    )
}
