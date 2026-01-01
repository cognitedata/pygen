# Overview over Tasks to Complete

These are tasks that are skipped from the first pass on a phase, but needs to be completed
before the v2.0 release. They are in now particular order, but has a number such
that they can be easily referenced.

1. ~~**Add DataModelResponseWithViews**~~
   - When passing `inline_views` when retrieving a DataModelResponse, the server response
     with a DataModelResponseWithViews should contain a list of `ViewResponses` instead of 
     `ViewReferences`. 
2. **Implement InstanceAPI.upsert(mode="update")**
  - This currently raises NotImplementedError, but should be implemented to allow
    updating existing instances without affecting other fields. The design of how to do this
    is not yet finalized.
3. **Ensure client name is set correctly in PygenClient**
   - This is to ensure appropriate logging and tracking on the server side.
