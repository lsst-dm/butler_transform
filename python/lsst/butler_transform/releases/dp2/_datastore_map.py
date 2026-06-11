# Mapping from physical paths, many of which are baked into the database as
# absolute URLs, to virtual "datastore" names.
DP2_DATASTORE_MAP = {
    "file:///sdf/group/rubin/repo/dp2_prep": "dp2",
    # This is a rucio alias for /sdf/data/rubin/repo/main_20210215/LSSTCam/calib
    "file:///sdf/data/rubin/rses/lsst/butlerdisk/rucio/repo/ancillary/LSSTCam/calib": "calib",
    "file:///sdf/data/rubin/shared/refcats": "refcats",
    "file:///sdf/data/rubin/lsstdata/offline/instrument/LSSTCam": "raw",
}
