from typing import Any
import pandas as pd
from typing import Dict, Tuple, Union

@pd.api.extensions.register_dataframe_accessor("Agg")
class AggDataframe:
    """DataFrame accessor for agg."""

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
        column: str,
        func: str,
    ) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[str, Any]]]:
        """
        Applies map over a dataframe.

        Args:
            column (str): The column to aggregate.
            func (str): The aggregation function.

        Returns:
            pd.DataFrame | tuple[pd.DataFrame, dict[str, Any]]: The result of the aggregation.
        """
        if func == "count" and column == "*":
            return self._obj.count()
        else:
            data = self._obj[column]
            if func == "sum":
                return data.sum()
            elif func == "mean":
                return data.mean()
            elif func == "count":
                return data.count()
            elif func == "min":
                return data.min()
            elif func == "max":
                return data.max()
            else:
                raise ValueError("Invalid aggregation function")
            
    