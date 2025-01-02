from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from typing import Dict, Any

class BaseModel(ABC):
    """Base class for all models with common functionality"""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary for database storage"""
        return asdict(self)
    
    @classmethod
    @abstractmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaseModel':
        """Create model instance from dictionary"""
        pass