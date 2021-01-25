import functools
import os

import scrapy
import scrapy.http.response.html
import scrapy.http.response.text


class GoogleGroupMBoxSpider(scrapy.Spider):
    """
    We use "?_escaped_fragment_=forum" trick to fetch plain HTML pages.

    Usage:

        scrapy runspider -a name=? ggmbox.py
    """
    name = "ggmbox"

    def __init__(self, name: str, template="{topic}/{index:03d}_{message}.email", output="{name}",
                 root="https://groups.google.com", prefix="", **kwargs):
        """
        Initializes a new instance of GoogleGroupMBoxSpider class.

        :param name: group name, e.g. "golang-nuts".
        :param template: `str.format()` raw email file name template. Supported keys: \
                         topic - topic identifier, \
                         index - message index in the thread, \
                         message - message identifier. \
                         The directories are automatically created.
        :param output: output directory.
        :param root: common root of all the URLs.
        :param kwargs: scrapy internal.
        """
        super().__init__(**kwargs)
        self.name = name
        self.output = output.format(name=name)
        self.template = template
        self.root = root
        if not prefix.endswith("/"):
            prefix += "/"
        self.prefix = prefix + "forum"
        self.start_urls = ["%s/%s/?_escaped_fragment_=forum/%s" % (self.root, self.prefix, name)]

    def parse(self, response: scrapy.http.response.html.HtmlResponse):
        for topic in response.css("tr a::attr(href)"):
            topic_url = "%s/%s/?_escaped_fragment_=topic/%s/%s" % (
                self.root, self.prefix, self.name, self.last_part(topic.extract()))
            yield response.follow(topic_url, self.parse_topic)

        for next_page in response.css("body > a"):
            self.log("Page: %s -> %s" % (
                self.last_part(response.url),
                self.last_part(next_page.css("::attr(href)").extract_first())))
            yield response.follow(next_page, self.parse)

    def parse_topic(self, response: scrapy.http.response.html.HtmlResponse):
        messages = []
        topic_id = self.last_part(response.url)
        for i, message in enumerate(response.css("tr")):
            topic_url = message.css("td[class=subject] > a::attr(href)").extract_first()
            if topic_url is None:
                continue
            message_id = self.last_part(topic_url)
            messages.append({
                "id": message_id,
                "author": message.css("td[class=author] ::text").extract_first(),
                "date": message.css("td[class=lastPostDate] ::text").extract_first(),
                "file": self.locate_email_file(topic_id, i, message_id, False)
            })
            file_name = self.locate_email_file(topic_id, i, message_id, True)
            if os.path.exists(file_name):
                self.log("Skipped %s/%s - already fetched" % (topic_id, message_id))
                continue
            yield response.follow(
                "%s/%s/message/raw?msg=%s/%s/%s" % (self.root, self.prefix, self.name,
                                                    topic_id, message_id),
                functools.partial(self.save_email, file_name=file_name))
        yield {"topic": response.css("h2 ::text").extract_first(),
               "id": topic_id,
               "messages": messages}

    def save_email(self, response: scrapy.http.response.text.TextResponse, file_name: str):
        with open(file_name, "wb") as fout:
            fout.write(response.body)

    @staticmethod
    def last_part(url):
        return url.rsplit("/", 1)[1]

    def locate_email_file(self, topic: str, index: int, message: str, full: bool):
        file_name = self.template.format(topic=topic, index=index, message=message)
        if full:
            file_name = os.path.join(self.output, file_name)
            file_dir = os.path.dirname(file_name)
            if not os.path.isdir(file_dir):
                os.makedirs(file_dir, exist_ok=True)
        return file_name
