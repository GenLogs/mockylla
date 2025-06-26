from collections.abc import Sequence


class Row(Sequence):
    def __init__(self, names, values):
        self._names = tuple(names)
        self._values = tuple(values)

        if len(self._names) != len(self._values):
            raise ValueError("Length of names and values must be the same.")

        for name, value in zip(self._names, self._values):
            setattr(self, name, value)

    def __getitem__(self, key):
        if isinstance(key, str):
            try:
                index = self._names.index(key)
                return self._values[index]
            except ValueError:
                raise KeyError(f"Column '{key}' not found in row.")
        elif isinstance(key, int):
            return self._values[key]
        elif isinstance(key, slice):
            return self._values[key]
        else:
            raise TypeError(
                f"Row indices must be integers, slices, or strings, not {type(key).__name__}"
            )

    def __len__(self):
        return len(self._values)

    def __repr__(self):
        return f"Row({', '.join(f'{n}={v!r}' for n, v in zip(self._names, self._values))})"

    def __eq__(self, other):
        if isinstance(other, dict):
            return self.as_dict() == other
        if isinstance(other, (list, tuple)):
            return self._values == tuple(other)
        return super().__eq__(other)

    def as_dict(self):
        return dict(zip(self._names, self._values))
