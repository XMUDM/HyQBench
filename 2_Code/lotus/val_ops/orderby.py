from typing import Any
import pandas as pd
from typing import Dict, Tuple, Union

@pd.api.extensions.register_dataframe_accessor("Orderby")
class OrderByDataframe:
    """DataFrame accessor for orderby."""

    def __init__(self, pandas_obj: Any):
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj: Any) -> None:
        # verify that the Series has the correct type
        if not isinstance(obj, pd.DataFrame):
            raise AttributeError("Must be a DataFrame")

    def __call__(
        self,
        column: list[str],
        ascending: bool = True,
    ) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[str, Any]]]:
        """
        Applies orderby over a dataframe.

        Args:
            column (list[str]): The column to sort by.
            ascending (bool): Whether to sort in ascending order. Defaults to True.

        Returns:
            pd.DataFrame | tuple[pd.DataFrame, dict[str, Any]]: The sorted dataframe.
        """
        data = self._obj
        if column[0] == '[':
            column = [i.strip() for i in column[1:-1].split(',')]
        else:
            column = [column]
        if ascending == "asc":
            ascending = True
        else:
            ascending = False
        for col in column:
            if col not in data.columns:
                raise ValueError(f"Column {col} not found in DataFrame")
        data = data.sort_values(by=column, ascending=ascending)
        return data
    