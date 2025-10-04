from typing import Any
import pandas as pd
from typing import Dict, Tuple, Union

@pd.api.extensions.register_dataframe_accessor("ColumnCalculate")
class ColumnCalculateDataframe:
    """DataFrame accessor for column calculate."""

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
        column1: str,
        column2: str,
        operation: str,
    ) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[str, Any]]]:
        """
        Applies calculate over a dataframe.

        Args:
            column1 (str): The first column.
            column2 (str): The second column.
            operation (str): The operation to apply.

        Returns:
            pd.DataFrame | tuple[pd.DataFrame, dict[str, Any]]: The resulting DataFrame.
        """
        data = self._obj
        if operation == "add":
            data["result"] = data[column1] + data[column2]
        elif operation == "sub":
            data["result"] = data[column1] - data[column2]
        elif operation == "mul":
            data["result"] = data[column1] * data[column2]
        elif operation == "div":
            data["result"] = data[column1] / data[column2]
        else:
            return "Invalid operation"
        
        return data
    