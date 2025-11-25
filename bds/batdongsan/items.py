import scrapy
from scrapy import Field


class BatdongsanItem(scrapy.Item):
    title = Field()
    description = Field()
    price = Field()
    square = Field()
    # address = Field()
    province = Field()
    district = Field()
    ward = Field()

    post_date = Field()
    link = Field()
    num_bedrooms = Field()
    num_floors = Field()
    num_toilets = Field()
