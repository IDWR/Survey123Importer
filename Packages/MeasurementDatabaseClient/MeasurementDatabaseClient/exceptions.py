class AlreadyGotOneException(Exception):
    """Exception raised when a row already exists with a particular HydrologyID and DiversionDate"""

    def __init__(self, hydrology_id, diversion_date, *errors):
        super(AlreadyGotOneException, self).__init__('Row already exists with HydrologyID={0} and DiversionDate={1}'.
                                                     format(hydrology_id, diversion_date))
        self.errors = errors


class InvalidDataException(Exception):
    """Exception raised when a row to be inserted does not contain values for all required fields"""

    def __init__(self, field_list: list, *errors):
        super(InvalidDataException, self).__init__("Invalid fields: {}".format(','.join(field_list)))
        self.field_list = field_list
        self.errors = errors
