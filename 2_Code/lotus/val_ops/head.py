from typing import Any
import pandas as pd
from typing import Dict, Tuple, Union

@pd.api.extensions.register_dataframe_accessor("Head")
class HeadDataframe:
    """DataFrame accessor for head."""

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
        n: int = 5,
    ) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[str, Any]]]:
        """
        Applies head over a dataframe.

        Args:
            n (int): The number of rows to return. Defaults to 5.

        Returns:
            pd.DataFrame | tuple[pd.DataFrame, dict[str, Any]]: The first n rows of the dataframe.
        """
        data = self._obj
        try:
            n = int(n)
        except ValueError:
            n = str(n)
        return data[:n]
    