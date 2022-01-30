import scrapy

class QuotesSpider(scrapy.Spider):
    name = 'posts_sgexpatsforum'

    start_urls = [
        'https://forum.singaporeexpats.com/viewforum.php?f=93'
    ]

    # Retrieve the list of topics and navigate into each topic page for multiple pages
    def parse(self, response):
        for topic_title in response.xpath('//div[@class="forumbg"]'):
            for topic in topic_title.xpath('div/ul[2]/li/dl/dt'):
                if topic is not None:
                    yield response.follow(topic.xpath('div/a/@href').get(), self.parse_post)

        next_page = response.xpath('//li[has-class("arrow next")]/a/@href').get()
        if next_page is not None:
            yield response.follow(next_page, self.parse)
                
    # Scrape the title of each topic, the respective author and the respective content for multiple pages
    def parse_post(self, response):
        for post in response.xpath('//div[contains(@id, "post")]'):
            content = post.xpath('div[@class="content"]/text()').get()
            if type(content) != type(None):
                content = content.strip()
            else:
                content = content

            yield {
                'title': post.xpath('h3/a/text()').get(),
                'author': post.xpath('p[@class="author"]/span/strong/a/text()').get(),
                'content': content
            }
        
            new_page = response.xpath('//li[has-class("arrow next")]/a/@href').get()
            if new_page is not None:
                yield response.follow(new_page, self.parse_post)
                