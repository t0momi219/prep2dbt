from enum import Enum

from sqlalchemy import DATE, DATETIME, INTEGER, REAL, TEXT


class TableauTypes(Enum):
    REAL = REAL
    DATE = DATE
    DATETIME = DATETIME
    INTEGER = INTEGER
    STRING = TEXT
