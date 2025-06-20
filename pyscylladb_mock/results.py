class ResultSet:
    def __init__(self, rows):
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)

    def __getitem__(self, key):
        return self._rows[key]

    def __len__(self):
        return len(self._rows)

    def one(self):
        try:
            return self._rows[0]
        except IndexError:
            return None

    def all(self):
        return list(self._rows)
