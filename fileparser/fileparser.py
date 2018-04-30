import trafaret as t
from time import sleep
from queue import Queue
from pprint import pprint
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor

from db import sa, dsn, Product


class FileParser(ABC):

    @abstractmethod
    def iterator(self):
        """
        Method that should iterate file. It is generator.
        Example of code:

        with gzip.open('files/products.xml.gz', 'r') as file:
            textbufer, append = '', False
            for line in file:
                if b'<item_data>' in line:
                    textbufer = line
                    append = True
                elif b'</item_data>' in line:
                    textbufer += line
                    append = False
                    yield xmltodict.parse(textbufer)
                    del textbufer
                elif append:
                    textbufer += line
        """
        yield

    @staticmethod
    @abstractmethod
    def converter(item):
        """
        Method to convert item data to formated data. Item is a result from iterator.
        Method must return a dictionary.
        Code example:

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
        """
        pass

    @staticmethod
    def validator(data):
        converter = t.Dict({
            t.Key('title') >> 'title': t.String(max_length=200),
            t.Key('sku_number', optional=True) >> 'sku_number': (t.Int | t.Null),
            t.Key('url') >> 'url': t.URL,
            t.Key('image_url') >> 'image_url': (t.URL | t.Null),
            t.Key('buy_url') >> 'buy_url': t.URL,
            t.Key('description') >> 'description': t.String,
            t.Key('discount', optional=True) >> 'discount': (t.String | t.Null),
            t.Key('discount_type', optional=True) >> 'discount_type': (t.String | t.Null),
            t.Key('currency') >> 'currency': t.String(max_length=3, min_length=3),
            t.Key('retail_price') >> 'retail_price': t.Float,
            t.Key('sale_price', optional=True) >> 'sale_price': (t.Float | t.Null),
            t.Key('brand', optional=True) >> 'brand': (t.String | t.Null),
            t.Key('manufacture', optional=True) >> 'manufacture': (t.String | t.Null),
            t.Key('shipping', optional=True) >> 'shipping': (t.Float | t.Null),
            t.Key('availability') >> 'availability': t.StrBool,
            t.Key('sizes', optional=True) >> 'sizes': (t.String | t.Null),
            t.Key('materials', optional=True) >> 'materials': (t.String | t.Null),
            t.Key('colors', optional=True) >> 'colors': (t.String | t.Null),
            t.Key('style', optional=True) >> 'style': (t.String | t.Null),
            t.Key('gender_group', optional=True) >> 'gender_group': (t.String | t.Null),
            t.Key('age_group', optional=True) >> 'age_group': (t.String | t.Null)
        })
        return converter.check(data)

    def get_id(self, item):
        """
        If item exists return them id
        :param item: dict
        :return: id or None
        """
        return self.exist_products.get(hash(item['title']))

    def worker(self, queue):
        """
        Thread Worker to work with DB

        :param queue: queue.Queue instance
        :return: None
        """
        with sa.create_engine(dsn).connect() as dbcon:
            while True:
                if queue.qsize() == 0:
                    sleep(1)
                    if queue.qsize() == 0:
                        break
                    continue
                item = queue.get()
                try:
                    if hash(item['title']) in self.exist_products:
                        dbcon.execute(Product.update().values(**item).where(Product.c.id == self.get_id(item)))
                    else:
                        result = dbcon.execute(Product.insert().values(**item))
                        self.exist_products[hash(item['title'])] = result.inserted_primary_key[0]
                except Exception as e:
                    print(type(e), e)

    def queue_loader(self, queue):
        """
        Loading Queue from file generator.
        If queue size is 100 and more, waiting, to save memory.

        :param queue: queue.Queue instance
        :return:
        """
        for item in self.iterator():
            try:
                converted_item = self.converter(item)
                valid_item = self.validator(converted_item)
            except Exception as e:
                print(type(e), e)
                continue
            queue.put(valid_item)
            while queue.qsize() > 100:
                sleep(0.2)

    def get_exists(self):
        """
        Load exists products hash to memory
        :return:
        """
        self.exist_products = {}
        limit = 100
        with sa.create_engine(dsn).connect() as dbcon:
            count = [x for x in dbcon.execute(Product.count())][0][0]
            for i in range(count//limit+1):
                sql = sa.select([Product.c.id, Product.c.title]).limit(limit).offset(limit*i)
                part = {hash(x[1]): x[0] for x in dbcon.execute(sql)}
                self.exist_products.update(part)

    def go(self, workers=10):
        """
        Main method
        :param workers: count of threads
        :return:
        """
        qu = Queue()
        self.get_exists()
        with ThreadPoolExecutor(max_workers=workers+1) as executor:
            executor.submit(self.queue_loader, qu)
            for _ in range(workers):
                executor.submit(self.worker, qu)

    def printone(self):
        """
        Just print one item from generator. It help test code.
        :return:
        """
        print('*' * 50)
        product = self.iterator().__next__()
        pprint(product)
        print('*'*50)
        converted_product = self.converter(product)
        pprint(converted_product)
        print('*' * 50)
        validated_product = self.validator(converted_product)
        pprint(validated_product)
        print('*' * 50)
