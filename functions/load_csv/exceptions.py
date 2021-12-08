from werkzeug.exceptions import HTTPException


class CsvNotFound(HTTPException):
    code = 404
    description = 'CSV file not found'


class SchemaNotFound(HTTPException):
    code = 404
    description = 'Schema file not found'


class CsvInvalid(HTTPException):
    code = 400
    description = 'CSV file invalid'


class SchemaInvalid(HTTPException):
    code = 400
    description = 'Schema file invalid'


class LoadJobError(HTTPException):
    code = 500
    description = 'Error during load job'
