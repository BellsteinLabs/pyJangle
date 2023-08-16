from functools import reduce

SEPARATOR = ","


class SqlLite3QueryBuilder:

    def __init__(self, table_name: str):
        self.table_name = table_name
        self._at = []
        self._upsert = []
        self._upsert_if_greater = []

    def at(self, column_name: str, value: any):
        self._at.append((column_name, value))
        return self

    def upsert(self, column_name: str, value: any, version_column_name: str = None, version_column_value: int = None):
        self._upsert.append(
            (column_name, value, version_column_name, version_column_value))
        return self

    def upsert_if_greater(self, column_name: str, value: any):
        self._upsert.append(
            (column_name, value))
        return self

    def done(self) -> tuple[str, tuple[any]]:
        query = ""
        params = ()
        # INSERT INTO
        query += "INSERT INTO ? "
        params += (self.table_name,)
        # ([col1],[col2],...[colN]) VALUES
        primary_key_column_names = reduce(
            lambda a, b: a + b, [(at_tuple[0],) for at_tuple in self._at])
        upsert_column_names = reduce(
            lambda a, b: a + b, [(upsert_tuple[0], upsert_tuple[2]) for upsert_tuple in self._upsert])
        upsert_if_greater_column_names = reduce(
            lambda a, b: a + b, [(upsert_tuple[0],) for upsert_tuple in self._upsert_if_greater])
        all_column_names = primary_key_column_names + \
            upsert_column_names + upsert_if_greater_column_names
        with_commas_parenthesis_and_values = "(" + \
            SEPARATOR.join(all_column_names) + ") VALUES "
        query += with_commas_parenthesis_and_values
        # (?, ?,...?)
        query += "(" + SEPARATOR.join(["?" for col in all_column_names]) + ") "
        primary_key_column_values = reduce(
            lambda a, b: a + b, [(at_tuple[1],) for at_tuple in self._at])
        upsert_column_values = reduce(
            lambda a, b: a + b, [(upsert_tuple[1], upsert_tuple[3]) for upsert_tuple in self._upsert])
        upsert_if_greater_column_values = reduce(
            lambda a, b: a + b, [(upsert_tuple[1],) for upsert_tuple in self._upsert_if_greater])
        params += primary_key_column_values + \
            upsert_column_values + upsert_if_greater_column_values
        # ON CONFLICT DO UPDATE SET
        query += "ON CONFLICT DO UPDATE SET "

        # for each upsert column
        #   [col_name] = CASE WHEN [ver_col_name] < [ver_col_value] OR [ver_col_name] IS NULL THEN [col_value] ELSE [col_name] END
        #   [ver_col_name] = CASE WHEN [ver_col_name] < [ver_col_value] OR [ver_col_name] IS NULL THEN [ver_col_value] ELSE [col_name] END
        #   OR
        #   [col_name] = [col_val]
        for tuple in self._upsert:
            col_name = tuple[0]
            col_value = tuple[1]
            ver_col_name = tuple[2]
            ver_col_value = tuple[3]
            if ver_col_name:
                query += col_name + " = CASE WHEN " + ver_col_name + " < ? OR " + \
                    ver_col_name + " IS NULL THEN ? ELSE " + col_name + " END "
                query += ver_col_name + " = CASE WHEN " + ver_col_name + " < ? OR " + \
                    ver_col_name + " IS NULL THEN ? ELSE " + ver_col_name + " END "
                params += (ver_col_value, col_value,
                           ver_col_value, ver_col_value)
            else:
                query += col_name + " = ?"
                params += (col_value)

        # for each upsert column
        #   [col_name] = CASE WHEN [col_name] < [col_value] OR [col_name] IS NULL THEN [col_value] ELSE [col_name] END
        for tuple in self._upsert_if_greater:
            col_name = tuple[0]
            col_value = tuple[1]
            query += col_name + " = CASE WHEN " + col_name + " < ? OR " + \
                col_name + " IS NULL THEN ? ELSE " + col_name + " END "
            params += (col_value, col_value)

        return (query, params)
