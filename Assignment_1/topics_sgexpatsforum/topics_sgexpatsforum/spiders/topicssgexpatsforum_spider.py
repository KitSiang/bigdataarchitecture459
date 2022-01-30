import scrapy

class QuotesSpider(scrapy.Spider):
    name = 'topics_sgexpatsforum'

    start_urls = [
        'https://forum.singaporeexpats.com/viewforum.php?f=93'
    ]

    # Scrape the list of topics' title, the respective number of replies and the respective number of views in multiple pages
    def parse(self, response):
        for topic_title in response.xpath('//div[@class="forumbg"]'):
            for topic in topic_title.xpath('div/ul[2]/li'):
                yield {
                    'title': topic.xpath('dl/dt/div/a/text()').get(),
                    'num_replies': topic.xpath('dl//dd[@class="posts"]/text()').get().rstrip(),
                    'num_views': topic.xpath('dl//dd[@class="views"]/text()').get().rstrip()
                }

        next_page = response.xpath('//li[@class="arrow next"]/a/@href').get()
        if next_page is not None:
            yield response.follow(next_page, self.parse)