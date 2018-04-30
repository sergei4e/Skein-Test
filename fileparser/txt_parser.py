import gzip
from .fileparser import FileParser


class TxtFileParser(FileParser):

    def __init__(self, filename, delimiter):
        self.filename = filename
        self.delimiter = delimiter

    def iterator(self):
        with gzip.open(self.filename, 'r') as file:
            file.__next__()
            for line in file:
                yield line.decode().split(self.delimiter)

    @staticmethod
    def converter(item):
        data = {
            "title": item[1],
            "sku_number": None,
            "url": item[5],
            "image_url": item[6],
            "buy_url": item[5],
            "description": item[9],
            "discount": None,
            "discount_type": item[22],
            "currency": item[25],
            "retail_price": item[24],
            "sale_price": None,
            "brand": item[16],
            "manufacture": None,
            "shipping": None,
            "availability": True,
            "sizes": item[30],
            "materials": None,
            "colors": None,
            "style": None,
            "gender_group": item[33],
            "age_group": None
        }
        return data
