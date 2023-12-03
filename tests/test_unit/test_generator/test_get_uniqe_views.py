from cognite.client import data_modeling as dm

#
# from cognite.pygen._core.logic import get_unique_views
#
#
# def test_get_unique_views(cog_pool_model: dm.DataModel, pygen_pool_model: dm.DataModel) -> None:
#     # Arrange
#     view_ids = [(view.space, view.external_id) for view in cog_pool_model.views] + [
#         (view.space, view.external_id) for view in pygen_pool_model.views
#     ]
#     duplicates = len(view_ids) - len(set(view_ids))
#
#     # Act
#     unique = get_unique_views(*cog_pool_model.views, *pygen_pool_model.views)
#
#     # Assert
#     assert len(unique) + duplicates == len(view_ids)
#     assert len(unique) == len({(view.space, view.external_id) for view in unique})
