import os
from fileparser import XmlFileParser, TxtFileParser


def main():
    for filename in os.listdir('files'):
        filename = f'files/{filename}'
        if 'xml' in filename:
            p = XmlFileParser(filename, 'item_data')
            p.go()
        elif 'txt' in filename:
            p = TxtFileParser(filename, '|')
            p.go()
        else:
            raise ValueError('Unknown file type')


if __name__ == '__main__':
    main()
