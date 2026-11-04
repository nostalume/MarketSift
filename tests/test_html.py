"""Information preservation at the single HTML boundary."""

import pytest

from marketsift.sources.html import clean_html, clean_node, parse_html


@pytest.mark.parametrize("encoding", ["utf-8-sig", "gb18030"])
def test_chinese_nested_tables_images_replies_and_original_tree(encoding):
    markup = """<article id="post"><script>embedded original</script><style>noise</style>
    <p>原文判断，成交额100.25万元。</p><blockquote>反证仍需观察</blockquote>
    <a href="/evidence">出处</a><img data-original="/chart.png" alt="量价图">
    <table><tr><th rowspan="2">行业</th><th colspan="2">观察</th></tr>
    <tr><td>49.0</td><td><table><tr><td>嵌套值-1.25</td></tr></table></td></tr></table>
    <div class="reply" id="reply-1">保留回复：第一条</div>
    <div class="reply" id="reply-2">保留回复：第二条</div></article>"""
    tree = parse_html(markup.encode(encoding), encoding=encoding)
    node = tree.select_one("article")
    result = clean_node(node, base_url="https://example.test/post")
    assert "embedded original" not in result["text"] and "noise" not in result["text"]
    assert tree.select_one("script").text == "embedded original"
    assert "49.0" in result["text"] and "嵌套值-1.25" in result["text"]
    assert 'rowspan="2"' in result["text"] and 'colspan="2"' in result["text"]
    assert "量价图" in result["text"] and "反证仍需观察" in result["text"]
    assert result["links"] == ["https://example.test/evidence"]
    assert result["attachments"] == ["https://example.test/chart.png"]
    replies = [
        clean_node(n, base_url="https://example.test/post")["text"]
        for n in tree.select(".reply")
    ]
    assert replies == ["保留回复：第一条", "保留回复：第二条"]
    assert tree.select_one("img").attrs.get("src") is None


def test_missing_body_does_not_fall_back_to_whole_page():
    result = clean_html(
        "<nav>无关导航</nav>", base_url="https://example.test", selector="article"
    )
    assert result["text"] == "" and result["issues"]
