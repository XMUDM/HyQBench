from typing import Any
import pandas as pd
import re
from typing import Dict, Tuple, Union

@pd.api.extensions.register_dataframe_accessor("Filter")
class FilterDataframe:
    """DataFrame accessor for filter."""

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
        op_list: str
    ) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[str, Any]]]:
        """
        Applies filter over a dataframe.

        Args:
            data (pd.DataFrame): The data to filter.
            op_list (str): The list of operators and values to filter by.

        Returns:
            pd.DataFrame | tuple[pd.DataFrame, dict[str, Any]]: The filtered dataframe or a tuple containing the filtered dataframe and statistics.
        """
        data = self._obj

        op_list = op_list.replace('[', '').replace(']', '').replace("(", "").replace(')', '')
        op_list = [i.strip() for i in op_list.split(',')]
        op_list = [(op_list[i], op_list[i+1]) for i in range(0, len(op_list), 2)]
        
        for op in op_list:
            operator = op[0]
            value = op[1]
            valid_operators = ["eq", "ne", "gt", "lt", "ge", "le", "like"]
            if operator not in valid_operators:
                raise ValueError(f"Operator '{operator}' is not supported.")
            if value == "nan":
                if operator in ["eq", "ne"]:
                    data = data[data[column].isnull()] if operator == "eq" else data[data[column].notnull()]
                else:
                    raise ValueError(f"Operator '{operator}' not supported for 'nan' value.")
            if data[column].dtype == "int64" or data[column].dtype == "float64":
                try:
                    if type(value) == str:
                        value = float(value)
                except ValueError:
                    raise ValueError(f"The value in column '{column}' is numeric.")
                if operator == "eq":
                    data = data[data[column] == value]
                elif operator == "ne":
                    data = data[data[column] != value]
                elif operator == "gt":
                    data = data[data[column] > value]
                elif operator == "lt":
                    data = data[data[column] < value]
                elif operator == "ge":
                    data = data[data[column] >= value]
                elif operator == "le":
                    data = data[data[column] <= value]  
            else:
                if operator == "eq":
                    data = data[data[column] == value]
                elif operator == "ne":
                    data = data[data[column] != value]
                elif operator == "like":
                    pattern = value.replace("%", ".*")
                    data = data[data[column].str.match(pattern, case=False, na=False)]
                else:
                    raise ValueError(f"Operator '{operator}' is not supported for non-numeric data.")
        
        return data
    