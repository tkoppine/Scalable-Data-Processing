# Assignment-1-tkoppine

### Query Performance Comparison

Both `WHERE` and `HAVING` queries produced identical results:

- **`WHERE` Query**: Filters rows before processing, which is generally more efficient.
- **`HAVING` Query**: Filters after grouping, but no aggregation was used here.

**Theory**: Using `WHERE` is preferred for non-aggregated data because it reduces the dataset earlier, leading to better performance.
