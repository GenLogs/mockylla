import re
from collections.abc import Sequence


class ResultSet(Sequence):
    """Lightweight stand-in for ``cassandra.cluster.ResultSet``."""

    batch_regex = re.compile(r"^\s*BEGIN\s+[a-zA-Z]*\s*BATCH")

    def __init__(
        self,
        rows,
        *,
        column_names=None,
        column_types=None,
        paging_state=None,
        has_more_pages=False,
        warnings=None,
        execution_info=None,
        response_future=None,
    ):
        self._all_rows = list(rows or [])
        self._position = 0
        self._column_names = (
            tuple(column_names) if column_names else self._infer_column_names()
        )
        self._column_types = column_types or {}
        self._paging_state = paging_state
        self._has_more_pages = has_more_pages
        self._warnings = list(warnings or [])
        self._execution_info = execution_info
        self.response_future = response_future

    def _infer_column_names(self):
        if not self._all_rows:
            return ()
        first = self._all_rows[0]
        names = getattr(first, "_names", None)
        if names:
            return tuple(names)
        if isinstance(first, dict):
            return tuple(first.keys())
        return ()

    def __iter__(self):
        return self

    def __next__(self):
        if self._position >= len(self._all_rows):
            raise StopIteration
        row = self._all_rows[self._position]
        self._position += 1
        return row

    # Python 2 compatibility alias used by the real driver.
    next = __next__

    def __getitem__(self, key):
        return self._all_rows[key]

    def __len__(self):
        return len(self._all_rows)

    def __bool__(self):
        return self._position < len(self._all_rows)

    __nonzero__ = __bool__

    def __eq__(self, other):
        if isinstance(other, ResultSet):
            return self._all_rows == other._all_rows
        if isinstance(other, (list, tuple)):
            return self._all_rows == list(other)
        return NotImplemented

    def one(self):
        try:
            return next(self)
        except StopIteration:
            return None

    def all(self):
        return list(self)

    @property
    def current_rows(self):
        return list(self._all_rows[self._position :])

    @property
    def has_more_pages(self):
        return self._has_more_pages

    @property
    def paging_state(self):
        return self._paging_state

    @property
    def column_names(self):
        return self._column_names

    @property
    def column_types(self):
        return self._column_types

    @property
    def warnings(self):
        return list(self._warnings)

    @property
    def execution_info(self):
        return self._execution_info

    def fetch_next_page(self):
        return []

    def cancel_continuous_paging(self):
        self._has_more_pages = False

    def get_query_trace(self, max_wait_sec=None):
        return None

    def get_all_query_traces(self, max_wait_sec_per=None):
        return []

    @property
    def was_applied(self):
        if not self._all_rows:
            return None

        first = self._all_rows[0]
        if isinstance(first, dict):
            return (
                bool(first.get("[applied]")) if "[applied]" in first else None
            )

        try:
            applied = first["[applied]"]
        except (KeyError, TypeError):
            return None
        return bool(applied)
