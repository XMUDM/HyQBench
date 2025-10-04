from typing import Any
import pandas as pd
from typing import Dict, Tuple, Union

@pd.api.extensions.register_dataframe_accessor("Groupby")
class GroupByDataframe:
    """DataFrame accessor for groupby."""

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
        group_column: list[str],
        agg_column: str,
        agg: str
    ) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[str, Any]]]:
        """
        Applies map over a dataframe.

        Args:
            column (list[str]): The column to group by.
            agg_column (str): The column to apply the aggregation function to.
            agg (str): The aggregation function to apply.

        Returns:
            pd.DataFrame | tuple[pd.DataFrame, dict[str, Any]]: The result of groupby.
        """
        data = self._obj
        if group_column[0] == '[':
            group_column = [i.strip() for i in group_column[1:-1].split(',')]
        else:
            group_column = [group_column]
        
        if agg == 'count': 
            return data.groupby(group_column)[agg_column].count().reset_index(name=f"{agg_column}_{agg}")
        else:
            if agg_column in group_column:
                return "Error: The aggregation column cannot be the same as the group column."
            return data.groupby(group_column)[agg_column].agg(agg).reset_index(name=f"{agg_column}_{agg}")
            
    