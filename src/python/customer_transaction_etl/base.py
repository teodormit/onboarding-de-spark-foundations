# src/python/ecommerce/transforms/base.py
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class TransformStep(ABC):
    """Interface for a DataFrame->DataFrame transformation."""
    name: str

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Return a new DataFrame with this step applied."""
        raise NotImplementedError
