from enum import Enum, auto

class DLTAsset(Enum):
  TABLE = auto()
  MATERIALIZED_VIEW = auto()
  VIEW = auto()