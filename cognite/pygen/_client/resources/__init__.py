"""Resource clients for CDF Data Modeling API.

This module provides resource-specific API clients for managing
CDF Data Modeling resources like spaces, data models, views, and containers.
"""

from ._base import BaseResourceAPI, Page
from ._containers import ContainersAPI
from ._data_models import DataModelsAPI
from ._spaces import SpacesAPI
from ._views import ViewsAPI

__all__ = [
    "BaseResourceAPI",
    "ContainersAPI",
    "DataModelsAPI",
    "Page",
    "SpacesAPI",
    "ViewsAPI",
]
