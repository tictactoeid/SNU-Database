class BooleanOperator:
    OPERATOR_TYPES = ["and", "or", "not"]
    def __init__(self, type, depth):
        self.type = type.lower()
        self.depth = depth
        if self.type not in self.OPERATOR_TYPES:
            raise("Boolean Operator should be one of AND/OR/NOT")
    def __str__(self):
        return self.type
class ComparisonOperand:
    # comparison_predicate | null_predicate
    # comparison_predicate : comp_operand comp_op comp_operand
    # comp_operand : comparable_value | [table_name "."] column_name
    # comparable_value : INT | STR | DATE | NULL

    COLUMN_NAME = "column_name"
    COMPARABLE_VALUE = "comparable_value"
    def __init__(self, type, table_name=None, column_name=None, value=None, comparable_value_type=None):
        self.type = type.lower()
        if self.type == self.COLUMN_NAME:
            if table_name:
                self.table_name = table_name.lower() # may be None
            else:
                self.table_name = None
            self.column_name = column_name.lower()
        elif self.type == self.COMPARABLE_VALUE:
            self.value = value
            self.comparable_value_type = comparable_value_type.lower()  # int, date, null / str -> char
            if self.comparable_value_type == "str":
                self.comparable_value_type = "char"
        else:
            raise("ComparisonOperand Type Error")
    def __str__(self):
        if self.type == self.COLUMN_NAME:
            if self.table_name:
                return f"{self.table_name}.{self.column_name}"
            else:
                return self.column_name
        else:
            return str(self.value)

class ComparisonPredicate:
    # operators
    LESSTHAN = "<"
    LESSEQUAL = "<="
    GREATERTHAN = ">"
    GREATEREQUAL = ">="
    EQUAL = "="
    NOTEQUAL = "!="
    def __init__(self, operand_1, operator, operand_2):
        self.operand_1 = operand_1
        self.operator = operator
        self.operand_2 = operand_2
    def __str__(self):
        return str(self.operand_1) +" "+ str(self.operator) +" "+ str(self.operand_2)
class NullPredicate:
    # null_predicate : [table_name "."] column_name null_operation
    # null_operation : IS [NOT] NULL
    def __init__(self, column_name, table_name=None, is_not_null=False):
        if table_name:
            self.table_name = table_name.lower()  # may be None
        self.column_name = column_name.lower()
        self.is_not_null = is_not_null # IS [NOT] NULL : NOT 있으면 True, 없으면 False

    def __str__(self):
        if self.is_not_null:
            if self.table_name:
                return f"{self.table_name}.{self.column_name} IS NOT NULL"
            else:
                return f"{self.column_name} IS NOT NULL"
        else:
            if self.table_name:
                return f"{self.table_name}.{self.column_name} IS NULL"
            else:
                return f"{self.column_name} IS NULL"