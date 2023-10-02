# A FSM Storage for aiogram 3.0+

It's a very simple FSM Storage for the aiogram 3.0+. It uses local SQLite database for storing states and data. It's possible to chose method of serializing data in the database. A 'pickle' or a 'json' are available. A 'pickle' is used as default. 

## Installation:
```bash
pip install aiogram-sqlite-storage
```

## Usage:
```python
from aiogram import Dispatcher

# Import SQLStorage class
from aiogram_sqlite_storage.sqlitestore import SQLStorage

# Initialise a storage
# Path to a database is optional. 'fsm_starage.db' will be created for default.
# 'serializing_method' is optional. 'pickle' is a default value. Also a 'json' is possible.
my_storage = SQLStorage('my_db_path.db', serializing_method = 'pickle')

# Initialize dispetcher with the storage
dp = Dispatcher(storage = my_storage)

# Use your FSM states and data as usual
```