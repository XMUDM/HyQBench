from typing import Any
import pandas as pd
from typing import Dict, Tuple, Union

@pd.api.extensions.register_dataframe_accessor("Join")
class JoinDataframe:
    """DataFrame accessor for join."""

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
        df2: pd.DataFrame,
        left_on: str,
        right_on: str,
    ) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[str, Any]]]:
        """
        Applies map over a dataframe.

        Args:
            df2 (pd.DataFrame): The dataframe to join.
            left_on (str): The column name for the left dataframe.
            right_on (str): The column name for the right dataframe.

        Returns:
            pd.DataFrame | tuple[pd.DataFrame, dict[str, Any]]: The joined dataframe.
        """
        data = self._obj
        return pd.merge(data, df2, left_on=left_on, right_on=right_on)
    