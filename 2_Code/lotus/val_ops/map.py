from typing import Any
import pandas as pd
from typing import Dict, Tuple, Union

@pd.api.extensions.register_dataframe_accessor("Map")
class MapDataframe:
    """DataFrame accessor for map."""

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
        column: list[str]
    ) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[str, Any]]]:
        """
        Applies map over a dataframe.

        Args:
            column (list[str]): The column to apply map on.

        Returns:
            pd.DataFrame | tuple[pd.DataFrame, dict[str, Any]]: The mapped dataframe or a tuple containing the mapped dataframe and statistics.
        """
        data = self._obj
        if column[0] == '[':
            column = [i.strip() for i in column[1:-1].split(',')]
        else:
            column = [column]
        return data[column]
    