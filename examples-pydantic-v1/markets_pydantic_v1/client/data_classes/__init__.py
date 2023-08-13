from markets_pydantic_v1.client.data_classes._bids import Bid, BidApply, BidList
from markets_pydantic_v1.client.data_classes._cog_bids import CogBid, CogBidApply, CogBidList
from markets_pydantic_v1.client.data_classes._cog_pools import CogPool, CogPoolApply, CogPoolList
from markets_pydantic_v1.client.data_classes._cog_process import CogProces, CogProcesApply, CogProcesList
from markets_pydantic_v1.client.data_classes._date_transformations import (
    DateTransformation,
    DateTransformationApply,
    DateTransformationList,
)
from markets_pydantic_v1.client.data_classes._date_transformation_pairs import (
    DateTransformationPair,
    DateTransformationPairApply,
    DateTransformationPairList,
)
from markets_pydantic_v1.client.data_classes._markets import Market, MarketApply, MarketList
from markets_pydantic_v1.client.data_classes._process import Proces, ProcesApply, ProcesList
from markets_pydantic_v1.client.data_classes._pygen_bids import PygenBid, PygenBidApply, PygenBidList
from markets_pydantic_v1.client.data_classes._pygen_pools import PygenPool, PygenPoolApply, PygenPoolList
from markets_pydantic_v1.client.data_classes._pygen_process import PygenProces, PygenProcesApply, PygenProcesList
from markets_pydantic_v1.client.data_classes._value_transformations import (
    ValueTransformation,
    ValueTransformationApply,
    ValueTransformationList,
)

BidApply.update_forward_refs(
    MarketApply=MarketApply,
)
CogBidApply.update_forward_refs(
    MarketApply=MarketApply,
)
CogProcesApply.update_forward_refs(
    BidApply=BidApply,
    DateTransformationPairApply=DateTransformationPairApply,
    ValueTransformationApply=ValueTransformationApply,
)
DateTransformationPairApply.update_forward_refs(
    DateTransformationApply=DateTransformationApply,
)
ProcesApply.update_forward_refs(
    BidApply=BidApply,
)
PygenBidApply.update_forward_refs(
    MarketApply=MarketApply,
)
PygenProcesApply.update_forward_refs(
    BidApply=BidApply,
    DateTransformationPairApply=DateTransformationPairApply,
    ValueTransformationApply=ValueTransformationApply,
)

__all__ = [
    "Bid",
    "BidApply",
    "BidList",
    "CogBid",
    "CogBidApply",
    "CogBidList",
    "CogPool",
    "CogPoolApply",
    "CogPoolList",
    "CogProces",
    "CogProcesApply",
    "CogProcesList",
    "DateTransformation",
    "DateTransformationApply",
    "DateTransformationList",
    "DateTransformationPair",
    "DateTransformationPairApply",
    "DateTransformationPairList",
    "Market",
    "MarketApply",
    "MarketList",
    "Proces",
    "ProcesApply",
    "ProcesList",
    "PygenBid",
    "PygenBidApply",
    "PygenBidList",
    "PygenPool",
    "PygenPoolApply",
    "PygenPoolList",
    "PygenProces",
    "PygenProcesApply",
    "PygenProcesList",
    "ValueTransformation",
    "ValueTransformationApply",
    "ValueTransformationList",
]
