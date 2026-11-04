"""PBC communication/news lists and source-selected article bodies."""

from . import pages

HOME = "https://www.pbc.gov.cn/goutongjiaoliu/113456/113469/index.html"


def parse(response):
    return pages.official(
        response,
        source="pbc",
        selector="#zoom",
        article_pattern=r"/113469/\d+/index\.html",
    )


def fetch(request, *, timeout=15, transport=None):
    yield from pages.articles(
        request, parse, [HOME], timeout=timeout, transport=transport
    )
