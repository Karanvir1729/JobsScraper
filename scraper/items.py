import scrapy


class ProviderItem(scrapy.Item):
    source = scrapy.Field()
    category = scrapy.Field()
    region = scrapy.Field()

    business_name = scrapy.Field()
    phone = scrapy.Field()
    email = scrapy.Field()
    website = scrapy.Field()
    address = scrapy.Field()
    city = scrapy.Field()
    province = scrapy.Field()
    postal_code = scrapy.Field()

    listing_url = scrapy.Field()
    detail_url = scrapy.Field()

