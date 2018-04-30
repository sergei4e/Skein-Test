import gzip
import xmltodict
from .fileparser import FileParser


class XmlFileParser(FileParser):

    def __init__(self, filename, tag_name):
        self.filename = filename
        self.xml_tag_open = f'<{tag_name}>'.encode()
        self.xml_tag_close = f'</{tag_name}>'.encode()

    def iterator(self):
        with gzip.open(self.filename, 'r') as file:
            textbufer, append = '', False
            for line in file:
                if self.xml_tag_open in line:
                    textbufer = line
                    append = True
                elif self.xml_tag_close in line:
                    textbufer += line
                    append = False
                    yield xmltodict.parse(textbufer)
                    del textbufer
                elif append:
                    textbufer += line

    @staticmethod
    def converter(item):
        data = {
            "title": item['item_data']['item_basic_data'].get('item_title'),
            "sku_number": item['item_data']['item_basic_data'].get('item_sku'),
            "url": item['item_data']['item_basic_data'].get('item_page_url'),
            "image_url": item['item_data']['item_basic_data'].get('item_image_url_large'),
            "buy_url": item['item_data']['item_basic_data'].get('offer_page_url'),
            "description": item['item_data']['item_basic_data'].get('item_long_desc'),
            "discount": None,
            "discount_type": None,
            "currency": 'USD',
            "retail_price": item['item_data']['item_basic_data'].get('item_price'),
            "sale_price": item['item_data']['item_basic_data'].get('tp_used_price'),
            "brand": item['item_data'].get('item_brand'),
            "manufacture": None,
            "shipping": item['item_data']['item_basic_data'].get('item_shipping_charge'),
            "availability": item['item_data']['item_basic_data'].get('item_is_fba'),
            "sizes": None,
            "materials": None,
            "colors": None,
            "style": None,
            "gender_group": None,
            "age_group": None
        }
        return data