langs = ["other", "en", "de", "ar", "bg", "ca", "cjk", "cz", "da", "el", "es", "et", "eu", "fa", "fi", "fr", "ga", "gl", "hi", "hu", "hy", "id", "it", "ja", "ko", "lv", "nl", "no", "pt", "ro", "ru", "sv", "th", "tr"]

def printSchema():
    print()
    print("Schema language fields:")
    for lang in langs:
        type = "general" if lang == "other" else lang
        print(f'<field name="content_txt_{lang}" type="text_{type}" multiValued="true" indexed="true" stored="true"/>')
        print(f'<field name="tags_txt_{lang}" type="text_{type}" multiValued="true" indexed="true" stored="true"/>')
        print(f'<field name="title_txt_{lang}" type="text_{type}" multiValued="false" indexed="true" stored="true"/>')
        print(f'<field name="subtitle_txt_{lang}" type="text_{type}" multiValued="false" indexed="true" stored="true"/>')
        print(f'<field name="summary_txt_{lang}" type="text_{type}" multiValued="false" indexed="true" stored="true"/>')
        print(f'<dynamicField name="*_page_txt_{lang}" type="text_{type}" multiValued="false" indexed="true" stored="true"/>')
        print(f'<copyField source="*_page_txt_{lang}" dest="content_txt_{lang}"/>')
        print(f'<copyField source="tags_txt_{lang}" dest="Keywords_facet"/>')
        print(f'<copyField source="*_txt_{lang}" dest="spellShingle"/>')

    print()
    print("Query field:")
    query = ""
    for lang in langs:
        type = "general" if lang == "other" else lang
        query += f' tags_txt_{lang}^3.0 title_txt_{lang}^2.5 subtitle_txt_{lang}^2.0 summary_txt_{lang}^1.5 content_txt_{lang}^1.0'

    print(f'<str name="qf">document^4.0 authors^3.5{query}</str>')

if __name__ == '__main__':
    printSchema()