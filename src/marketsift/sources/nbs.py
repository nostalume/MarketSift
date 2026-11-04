"""NBS releases, including native tables and publication precision."""

from . import pages

HOME = "https://www.stats.gov.cn/sj/zxfb/"


def parse(response):
    return pages.official(
        response,
        source="nbs",
        selector=".detail-text-content .txt-content",
        article_pattern=r"/sj/zxfb/\d+/t\d+_\d+\.html",
    )


def fetch(request, *, timeout=15, transport=None):
    yield from pages.articles(
        request, parse, [HOME], timeout=timeout, transport=transport
    )
