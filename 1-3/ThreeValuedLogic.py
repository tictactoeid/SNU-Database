class ThreeValuedLogic():
    TRUE = "true"
    FALSE = "false"
    UNKNOWN = "unknown"
    inputs = [TRUE, FALSE, UNKNOWN]
    def ThreeValuedOR(self, operand_1, operand_2):
        if operand_1 not in self.inputs or operand_2 not in self.inputs:
            raise Exception("Wrong input(s) for Three-Valued Logic")
        if operand_1 != self.UNKNOWN and operand_2 != self.UNKNOWN:
            if operand_1 == self.TRUE or operand_2 == self.TRUE:
                return self.TRUE
            else:
                return self.FALSE
        elif (operand_1 == self.TRUE and operand_2 == self.UNKNOWN) or (operand_1 == self.UNKNOWN and operand_2 == self.TRUE):
            return self.TRUE
        else:
            return self.UNKNOWN
    def ThreeValuedAND(self, operand_1, operand_2):
        if operand_1 not in self.inputs or operand_2 not in self.inputs:
            raise Exception("Wrong input(s) for Three-Valued Logic")
        if operand_1 != self.UNKNOWN and operand_2 != self.UNKNOWN:
            if operand_1 == self.FALSE or operand_2 == self.FALSE:
                return self.FALSE
            else:
                return self.TRUE
        elif (operand_1 == self.FALSE and operand_2 == self.UNKNOWN) or (operand_1 == self.UNKNOWN and operand_2 == self.FALSE):
            return self.FALSE
        else:
            return self.UNKNOWN
    def ThreeValuedNOT(self, operand):
        if operand not in self.inputs:
            raise Exception("Wrong input(s) for Three-Valued Logic")

        if operand == self.TRUE:
            return self.FALSE
        elif operand == self.FALSE:
            return self.TRUE
        else:
            return self.UNKNOWN






