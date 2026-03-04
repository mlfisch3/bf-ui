from ui.tracker_engine import parse_listing_rows, parse_thread_numeric_id_from_href


def test_parse_thread_numeric_id_from_href() -> None:
    href = "/threads/creely-blades-mako-pg-magnacut-g10.2067309/"
    assert parse_thread_numeric_id_from_href(href) == "2067309"


def test_parse_listing_rows_extracts_id_and_views() -> None:
    html = """
    <article class='structItem structItem--thread'>
      <div class='structItem-title'>
        <a href='/threads/example-title.1234567/'>Example</a>
      </div>
      <dl class='pairs pairs--justified'>
        <dt>Views</dt><dd>1.2K</dd>
      </dl>
    </article>
    """
    rows = parse_listing_rows(html)
    assert rows
    assert rows[0]["thread_numeric_id"] == "1234567"
    assert rows[0]["views"] == 1200
