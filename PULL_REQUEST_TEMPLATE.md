# Fix search crash when genre_stations is None

## Summary
This patch fixes a critical bug where the search function would crash with a TypeError when Pandora's API returns no genre stations.

## Problem
The search function in `library.py` was attempting to iterate directly over `search_result.genre_stations` without checking if it exists or is None. This would cause a TypeError and crash the search functionality.

```python
# Before (would crash if genre_stations is None):
for genre in search_result.genre_stations:
    tracks.append(...)
```

## Solution
- Added a null check (`if search_result.genre_stations:`) before iterating
- Changed search parameters to improve results:
  - `include_near_matches`: Changed from False to True for better search coverage
  - `include_genre_stations`: Changed from True to False as it was causing issues

```python
# After (safe iteration):
if search_result.genre_stations:
    for genre in search_result.genre_stations:
        tracks.append(...)
```

## Testing
- Tested with Mopidy 3.4.2 on Raspberry Pi
- Search no longer crashes when genre_stations is None
- Successfully browses 96 stations
- Search functionality works reliably

## Impact
This is a critical fix that prevents the extension from crashing during search operations. Without this patch, users may experience complete search failures in certain scenarios.

## Type of Change
- [x] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)

## Checklist
- [x] Code follows the project's style guidelines
- [x] Self-review of code completed
- [x] Tested on actual hardware (Raspberry Pi)
- [x] No unnecessary comments or debug code included