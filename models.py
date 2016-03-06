from peewee import *

# use this, because our db connection is created during runtime
database_proxy = Proxy()


class BaseModel(Model):
    class Meta:
        database = database_proxy


class FactData(Model):
    """
    FactData model
    """
    class Meta:
        db_table = "facts_data"

    DOMAIN_DESC = CharField()
    COMMODITY_DESC = CharField()
    STATISTICCAT_DESC = CharField()
    AGG_LEVEL_DESC = CharField()
    COUNTRY_NAME = CharField()
    STATE_NAME = CharField()
    COUNTY_NAME = CharField()
    UNIT_DESC = CharField()
    VALUE = CharField()
    YEAR = CharField()
