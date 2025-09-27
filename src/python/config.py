import os
from dataclasses import dataclass

@dataclass
class Paths:
    data_root: str = os.getenv("DATA_ROOT", "./data")
    raw: str = os.path.join(data_root, "raw")
    silver: str = os.path.join(data_root, "silver")
    gold: str = os.path.join(data_root, "gold")

@dataclass
class Options:
    top_n_products: int = int(os.getenv("TOP_N", "10"))
    #partitions_col: str = "purchase_date"

paths = Paths()
opts = Options()