import pytest

from mockylla.results import ResultSet
from mockylla.row import Row


def _make_rows(count=3):
    return [Row(["id", "value"], [i, f"row-{i}"]) for i in range(count)]


def test_one_consumes_rows():
    result = ResultSet(_make_rows(2))

    first = result.one()
    second = result.one()

    assert first.id == 0
    assert second.id == 1
    assert result.one() is None


def test_all_returns_remaining_rows():
    result = ResultSet(_make_rows(3))
    _ = result.one()

    remaining = result.all()

    assert [row.id for row in remaining] == [1, 2]
    assert result.one() is None


def test_current_rows_tracks_remaining_rows():
    result = ResultSet(_make_rows(2))

    assert [row.id for row in result.current_rows] == [0, 1]

    _ = result.one()

    assert [row.id for row in result.current_rows] == [1]


def test_was_applied_detects_lwt_result():
    applied_row = Row(["[applied]", "id"], [True, 1])
    rejected_row = Row(["[applied]", "id"], [False, 1])

    assert ResultSet([applied_row]).was_applied is True
    assert ResultSet([rejected_row]).was_applied is False
    assert ResultSet([]).was_applied is None
    assert ResultSet(_make_rows(1)).was_applied is None


def test_getitem_and_len_reflect_all_rows():
    rows = _make_rows(2)
    result = ResultSet(rows)

    assert len(result) == 2
    assert result[0] == rows[0]
    assert result[:] == rows


def test_boolean_represents_remaining_rows():
    result = ResultSet(_make_rows(1))
    assert bool(result)

    _ = result.one()
    assert bool(result) is False


def test_column_names_inferred_from_rows():
    result = ResultSet([Row(["id", "value"], [1, "a"])])
    assert result.column_names == ("id", "value")


@pytest.mark.parametrize(
    "method, expected",
    [
        ("fetch_next_page", []),
        ("get_all_query_traces", []),
        ("get_query_trace", None),
    ],
)
def test_stub_methods_return_harmless_defaults(method, expected):
    result = ResultSet(_make_rows(1))
    call = getattr(result, method)
    assert call() == expected


def test_cancel_continuous_paging_is_noop():
    result = ResultSet(_make_rows(1))
    result.cancel_continuous_paging()

    assert result.has_more_pages is False
    assert result.paging_state is None
