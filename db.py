import sys
import sqlalchemy as sa
from datetime import datetime

# connection = {'user': 'user', 'database': 'db', 'host': 'host', 'password': 'pass'}
# dsn = 'postgresql://{user}:{password}@{host}/{database}'.format(**connection)

dsn = 'sqlite:///database.db'

metadata = sa.MetaData()

Product = sa.Table(
    "products", metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("title", sa.String),
    sa.Column("sku_number", sa.Integer),
    sa.Column("url", sa.String, unique=True),
    sa.Column("image_url", sa.String),
    sa.Column("buy_url", sa.String),
    sa.Column("description", sa.Text),
    sa.Column("discount", sa.String),
    sa.Column("discount_type", sa.String),
    sa.Column("currency", sa.String(3)),
    sa.Column("retail_price", sa.Numeric),
    sa.Column("sale_price", sa.Numeric),
    sa.Column("brand", sa.String),
    sa.Column("manufacture", sa.String),
    sa.Column("shipping", sa.Numeric),
    sa.Column("availability", sa.Boolean),
    sa.Column("sizes", sa.String),
    sa.Column("materials", sa.String),
    sa.Column("colors", sa.String),
    sa.Column("style", sa.String),
    sa.Column("gender_group", sa.String),
    sa.Column("age_group", sa.String),
    sa.Column('timestamp', sa.DateTime, default=datetime.now)
)


if __name__ == '__main__':
    engine = sa.create_engine(dsn)
    if 'create' in sys.argv:
        metadata.create_all(engine)
        print('All tables was created successfully')
    elif 'drop' in sys.argv:
        metadata.drop_all(engine)
        print('All tables was deleted successfully')
    else:
        raise AssertionError('Need `create` or `drop` parameter')
