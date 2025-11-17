# Active TODOs

**Last Updated**: 2025-11-17

## High Priority

### DELETE Operation
- [ ] Debug manifest processing issue (zip_eq panic in 4/11 tests)
- [ ] Fix row-level predicate evaluation (currently file-level only)
- [ ] Add CopyOnWrite strategy (optional)
- [ ] Add Auto mode statistics-based selection

### Compaction (Week 4 - Current)
- [x] Data rewriting infrastructure ✅
- [x] Implement Parquet writer setup ✅
- [ ] Implement selective file reading (FileScanTask creation)
- [ ] Implement data combining/batch merging logic
- [ ] Complete end-to-end rewrite with actual data
- [ ] Add progress tracking and metrics
- [ ] Write unit tests for rewrite logic

## Medium Priority

### Compaction (Week 5-7)
- [ ] Build manifests for compacted files
- [ ] Create snapshot with Operation::Replace
- [ ] Integration tests
- [ ] Java compatibility tests

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
