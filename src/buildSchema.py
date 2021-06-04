import config
# from solr import Solr

langs = ["other", "en", "de", "ar", "bg", "ca", "cjk", "cz", "da", "el", "es", "et", "eu", "fa", "fi", "fr", "ga", "gl", "hi", "hu", "hy", "id", "it", "ja", "ko", "lv", "nl", "no", "pt", "ro", "ru", "sv", "th", "tr"]
# solr = Solr(config.SOLR_HOST, config.SOLR_PORT, config.SOLR_CORE)

# solr.buildSchema(langs)

for lang in langs:
    type = "general" if lang == "other" else lang
    print(f'<field name="content_txt_{lang}" type="text_{type}" multiValued="true" indexed="true" stored="true"/>')
    print(f'<field name="tags_txt_{lang}" type="text_{type}" multiValued="true" indexed="true" stored="true"/>')
    print(f'<dynamicField name="*_page_txt_{lang}" type="text_{type}" multiValued="false" indexed="true" stored="true"/>')
    print(f'<copyField source="*_page_txt_{lang}" dest="content_txt_{lang}"/>')
    print(f'<copyField source="tags_txt_{lang}" dest="Keywords_facet"/>')
    print(f'<copyField source="*_txt_{lang}" dest="spellShingle"/>')