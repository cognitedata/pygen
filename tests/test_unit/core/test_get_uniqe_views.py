from cognite.client import data_modeling as dm

from cognite.pygen._core.dms_to_python import get_unique_views


def test_get_unique_views(cog_pool_model: dm.DataModel, pygen_pool_model: dm.DataModel) -> None:
    # Arrange
    expected = dm.ViewList(cog_pool_model.views)
    for views_to_add in ["PygenPool", "PygenBid", "PygenProcess"]:
        view = next(v for v in pygen_pool_model.views if v.external_id == views_to_add)
        expected.append(view)

    # Act
    actual = get_unique_views(*cog_pool_model.views, *pygen_pool_model.views)

    # Assert
    assert actual == expected
