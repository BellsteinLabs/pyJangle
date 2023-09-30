"Utilities to assist in creating SQL queries."

from functools import reduce

SEPARATOR = ","


class Sqlite3QueryBuilder:
    """Builds commonly general-purpose upsert sqlite queries.
    
    When updating database tables with events, it must be assumed that events could 
    potentially be duplicated or out of order, or both.  One technique for handling this
    is to accompany each data field with a version field which keeps track of the 
    version number of the last event to update the field.  Any upserts consider the 
    version field when determining if an override of the existing value is appropriate.
    The general form of such a query is as follows:

    INSERT INTO table_name 
        (col_primary_key, col_a, col_a_version, col_b, col_b_version, etc...)
    VALUES        
        (primary_key, a, a_version, b, b_version)
    ON CONFLICT DO UPDATE SET
        col_a = CASE WHEN col_a_version < a_version OR col_a_version IS NULL 
            THEN a ELSE col_a END
        col_a_version = CASE WHEN col_a_version < a_version OR col_a_version IS NULL 
            THEN a_version ELSE col_a_version END
        col_b = CASE WHEN col_b_version < b_version OR col_b_version IS NULL 
            THEN b ELSE col_b END
        col_b_version = CASE WHEN col_b_version < b_version OR col_b_version IS NULL 
            THEN b_version ELSE col_b_version END
    
    An additional UPDATE SET clause which is useful in certain situations is available.  
    It does not require a version column and updates a value only if the new one is 
    larger.  This can be useful for status fields where status codes always become 
    increasingly larger:

        col_a = CASE WHEN col_a < a OR col_a IS NULL THEN a ELSE col_a END

    Use `at` to specify a primary key.  Make additional calls to `at` for compound keys.
    Use `upsert` to specify a column and its corresponding version column.
    Use `upsert_if_greater` to specify a column that should be updated if the new value 
    is larger.
    Use `done` to compile the query.

    This class uses the builder pattern.  Example follows:

        query = Sqlite3QueryBuilder("table_name") \
            .at("transaction_id", event.transaction_id) \
            .upsert("funding_account", event.funding_account_id) \
            .upsert("amount", event.amount) \
            .upsert_if_greater("state", TRANSACTION_STATES.REQUEST_DEBITED) \
            .done()
    """

    def __init__(self, table_name: str):
        """Initialize a new query builder.

        Args:
            table_name:
                The name of the table to update.
        """
        self.table_name = table_name
        self._at = []
        self._upsert = []
        self._upsert_if_greater = []

    def at(self, column_name: str, value: any):
        """Specify a primary key column.

        For compound keys, make one call to this method for each column.

        Args:
            column_name:
                The name of the PK column.
            value:
                The value of the PK.
        """
        self._at.append((column_name, value))
        return self

    def upsert(
        self,
        column_name: str,
        value: any,
        version_column_name: str = None,
        version_column_value: int = None,
    ):
        """Specify a column to potentially update with a new value based on version.

        Args:
            column_name:
                The name of the column to update.
            value:
                The new value for `column_name`.
            version_column_name:
                Name of the column holding the value of the version of the last event to
                update `column_name`.
            version_column_value:
                The value of the version field of the current event that is attempting
                to update `column_name`.
        """
        if version_column_name:
            self._upsert.append(
                (column_name, value, version_column_name, version_column_value)
            )
        else:
            self._upsert.append((column_name, value))
        return self

    def upsert_if_greater(self, column_name: str, value: any):
        """Specify a column to potentially update with a new value based on magnitude.

        If `value` is greater than the current value of `column_name`, an update will
        occur.

        Args:
            column_name:
                The name of the column to update.
            value:
                The new value for `column_name`.
        """
        self._upsert.append((column_name, value))
        return self

    def done(self) -> tuple[str, tuple[any]]:
        "Returns a completed query."

        query = ""
        params = ()
        # INSERT INTO
        query += f"INSERT INTO {self.table_name} "
        # ([col1],[col2],...[colN]) VALUES
        primary_key_column_names = reduce(
            lambda a, b: a + b, [(at_tuple[0],) for at_tuple in self._at]
        )
        upsert_column_names = reduce(
            lambda a, b: a + b,
            [
                (upsert_tuple[0], upsert_tuple[2])
                if len(upsert_tuple) == 4
                else (upsert_tuple[0],)
                for upsert_tuple in self._upsert
            ],
        )
        upsert_if_greater_column_names = (
            reduce(
                lambda a, b: a + b,
                [(upsert_tuple[0],) for upsert_tuple in self._upsert_if_greater],
            )
            if self._upsert_if_greater
            else tuple()
        )
        all_column_names = (
            primary_key_column_names
            + upsert_column_names
            + upsert_if_greater_column_names
        )
        with_commas_parenthesis_and_values = (
            "(" + SEPARATOR.join(all_column_names) + ") VALUES "
        )
        query += with_commas_parenthesis_and_values
        # (?, ?,...?)
        query += "(" + SEPARATOR.join(["?" for col in all_column_names]) + ") "
        primary_key_column_values = reduce(
            lambda a, b: a + b, [(at_tuple[1],) for at_tuple in self._at]
        )
        upsert_column_values = reduce(
            lambda a, b: a + b,
            [
                (upsert_tuple[1], upsert_tuple[3])
                if len(upsert_tuple) == 4
                else (upsert_tuple[1],)
                for upsert_tuple in self._upsert
            ],
        )
        upsert_if_greater_column_values = (
            reduce(
                lambda a, b: a + b,
                [(upsert_tuple[1],) for upsert_tuple in self._upsert_if_greater],
            )
            if self._upsert_if_greater
            else tuple()
        )
        params += (
            primary_key_column_values
            + upsert_column_values
            + upsert_if_greater_column_values
        )
        # ON CONFLICT DO UPDATE SET
        query += "ON CONFLICT DO UPDATE SET "

        # for each upsert column
        #   [col_name] = CASE WHEN [ver_col_name] < [ver_col_value] OR [ver_col_name]
        #     IS NULL THEN [col_value] ELSE [col_name] END
        #   [ver_col_name] = CASE WHEN [ver_col_name] < [ver_col_value] OR
        #     [ver_col_name] IS NULL THEN [ver_col_value] ELSE [col_name] END
        #   OR
        #   [col_name] = [col_val]
        for tupl in self._upsert:
            col_name = tupl[0]
            col_value = tupl[1]
            ver_col_name = tupl[2] if len(tupl) > 2 else None
            ver_col_value = tupl[3] if len(tupl) > 2 else None
            if ver_col_name:
                query += (
                    col_name
                    + " = CASE WHEN "
                    + ver_col_name
                    + " < ? OR "
                    + ver_col_name
                    + " IS NULL THEN ? ELSE "
                    + col_name
                    + " END, "
                )
                query += (
                    ver_col_name
                    + " = CASE WHEN "
                    + ver_col_name
                    + " < ? OR "
                    + ver_col_name
                    + " IS NULL THEN ? ELSE "
                    + ver_col_name
                    + " END, "
                )
                params += (ver_col_value, col_value, ver_col_value, ver_col_value)
            else:
                query += col_name + " = ?, "
                params += (col_value,)

        # for each upsert column
        #   [col_name] = CASE WHEN [col_name] < [col_value] OR [col_name] IS NULL THEN
        #     [col_value] ELSE [col_name] END
        for tupl in self._upsert_if_greater:
            col_name = tupl[0]
            col_value = tupl[1]
            query += (
                col_name
                + " = CASE WHEN "
                + col_name
                + " < ? OR "
                + col_name
                + " IS NULL THEN ? ELSE "
                + col_name
                + " END "
            )
            params += (col_value, col_value)

        return (query.strip().rstrip(","), params)
