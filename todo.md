# Active TODOs

**Last Updated**: 2025-11-17

## High Priority

### DELETE Operation
- [ ] Debug manifest processing issue (zip_eq panic in 4/11 tests)
- [ ] Fix row-level predicate evaluation (currently file-level only)
- [ ] Add CopyOnWrite strategy (optional)
- [ ] Add Auto mode statistics-based selection

### Compaction (Weeks 4-5 - Complete ✅)
- [x] Data rewriting infrastructure ✅
- [x] Implement Parquet writer setup ✅
- [x] Implement selective file reading (FileScanTask creation) ✅
- [x] Implement data combining/batch merging logic ✅
- [x] Complete end-to-end rewrite with actual data ✅
- [x] Build manifests for compacted files ✅
- [x] Create snapshot with Operation::Replace ✅
- [x] Load existing manifest entries and preserve sequence numbers ✅

## Medium Priority

### Compaction (Week 6-7 - Testing)
- [ ] Write unit tests for commit() flow
- [ ] Integration tests (end-to-end compaction)
- [ ] Java compatibility tests
- [ ] Add progress tracking and metrics (optional)

### Testing Infrastructure
- [ ] Fix pre-existing manifest field processing issue
- [ ] Verify all DELETE tests pass after fix

## Completed ✅

- [x] DELETE API design (Session 2)
- [x] DELETE file scanning (Session 3)
- [x] DELETE MergeOnRead complete (Session 4)
- [x] DELETE integration tests (Session 5)
- [x] RecordBatchTransformer test fix (Session 6)
- [x] Compaction design & bin packing (Phase 2 Week 1-2)
- [x] Compaction file analysis (Phase 2 Week 3)
