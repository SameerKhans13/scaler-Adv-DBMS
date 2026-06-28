---
"minidb-groupby": patch
---
Fixed a critical bug where soft-deleted records could reappear after a crash but before recovery by aligning non-transactional SQL scans with MVCC transaction visibility guidelines. Also fixed a bug in simulateCrash to reuse the correct File/Browser StorageProvider and correctly rebuild the B+ Tree index from post-crash pages.
