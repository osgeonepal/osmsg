"""Package-level smoke tests."""

from osmsg.exceptions import NoDataFoundError, OsmsgError


def test_no_data_found_is_not_osmsg_error():
    """Catching OsmsgError must not silently swallow 'no data' (an info condition, not a failure)."""
    assert not issubclass(NoDataFoundError, OsmsgError)
    assert issubclass(NoDataFoundError, Exception)
