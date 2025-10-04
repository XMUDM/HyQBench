def ValueCalculate(
        operation: str,
        value1: str,
        value2: str = None
    ) -> float:
        """
        Applies calculate over values.

        Args:
            operation (str): The operation to apply.
            value1 (str): The first value to apply the operation to.
            value2 (str): The second value to apply the operation to.

        Returns:
            float: The result of the operation.
        """
        try:
            value1 = float(value1)
        except:
            raise ValueError("This operator is a numerical computation operator, and its usage is operator;;value;;....")

        if value2 is not None:
            if operation == 'round':
                raise ValueError("This operator needs only one value to perform the operation.")
            try:
                value2 = float(value2)
            except:
                raise ValueError("This operator is a numerical computation operator, and its usage is operator;;value;;....")
            
            if operation == 'add':
                result = value1 + value2
            elif operation == 'sub':
                result = value1 - value2
            elif operation == 'mul':
                result = value1 * value2
            elif operation == 'div':
                result = value1 / value2
            
        else:
            if operation == 'round':
                result = round(value1)
            else:
                raise ValueError("This operator needs two values to perform the operation.")
            
        return result
