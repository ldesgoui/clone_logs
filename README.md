# clone\_logs

### Maintain a local clone of the logs.tf dataset for analysis

**It is greatly recommended to seek existing cloned databases instead of fetching a large amount off the logs.tf API.**

Some should be available here: https://mega.nz/#F!l9oGiKCb!lTWT2RSkTYv-TJZb92_ksA


## Installation:

- Install Python 3
- Download `clone_logs.py` (there are no external dependencies)
- Optional: Download an existing cloned databaseo


## Usage:

```
./clone_logs.py --help
./clone_logs.py --import archive/*.sqlite3
./clone_logs.py --limit 1000
./clone_logs.py -l 1000 --player 76561197963314359 --map cp_badlands
```
