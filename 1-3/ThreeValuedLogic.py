class ThreeValuedLogic:
    TRUE = "true"
    FALSE = "false"
    UNKNOWN = "unknown"
    NULL = "null"
    inputs = [TRUE, FALSE, UNKNOWN]
    @staticmethod
    def ThreeValuedOR(operand_1, operand_2):
        if operand_1 not in ThreeValuedLogic.inputs or operand_2 not in ThreeValuedLogic.inputs:
            raise Exception("Wrong input(s) for Three-Valued Logic")
        if operand_1 != ThreeValuedLogic.UNKNOWN and operand_2 != ThreeValuedLogic.UNKNOWN:
            if operand_1 == ThreeValuedLogic.TRUE or operand_2 == ThreeValuedLogic.TRUE:
                return ThreeValuedLogic.TRUE
            else:
                return ThreeValuedLogic.FALSE
        elif (operand_1 == ThreeValuedLogic.TRUE and operand_2 == ThreeValuedLogic.UNKNOWN) or (operand_1 == ThreeValuedLogic.UNKNOWN and operand_2 == ThreeValuedLogic.TRUE):
            return ThreeValuedLogic.TRUE
        else:
            return ThreeValuedLogic.UNKNOWN
    @staticmethod
    def ThreeValuedAND(operand_1, operand_2):
        if operand_1 not in ThreeValuedLogic.inputs or operand_2 not in ThreeValuedLogic.inputs:
            raise Exception("Wrong input(s) for Three-Valued Logic")
        if operand_1 != ThreeValuedLogic.UNKNOWN and operand_2 != ThreeValuedLogic.UNKNOWN:
            if operand_1 == ThreeValuedLogic.FALSE or operand_2 == ThreeValuedLogic.FALSE:
                return ThreeValuedLogic.FALSE
            else:
                return ThreeValuedLogic.TRUE
        elif (operand_1 == ThreeValuedLogic.FALSE and operand_2 == ThreeValuedLogic.UNKNOWN) or (operand_1 == ThreeValuedLogic.UNKNOWN and operand_2 == ThreeValuedLogic.FALSE):
            return ThreeValuedLogic.FALSE
        else:
            return ThreeValuedLogic.UNKNOWN
    @staticmethod
    def ThreeValuedNOT(operand):
        if operand not in ThreeValuedLogic.inputs:
            raise Exception("Wrong input(s) for Three-Valued Logic")

        if operand == ThreeValuedLogic.TRUE:
            return ThreeValuedLogic.FALSE
        elif operand == ThreeValuedLogic.FALSE:
            return ThreeValuedLogic.TRUE
        else:
            return ThreeValuedLogic.UNKNOWN





