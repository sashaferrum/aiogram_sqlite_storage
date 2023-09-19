# A FSM Storage for aiogram 3.0+

It's a very simple FSM Storage for the aiogram 3.0+. It uses SQLite database for storing states and data. And usig 'pickle' libruary for serializing objects. It's sutable for small and simple projects.

## Installetion:
```bash
pip install aiogram-sqlite-storage
```

## Usage:
```python
# Import SQLStorage class
from aiogram_sqlite_storage.sqlitestore import SQLStorage
from aiogram import Dispatcher

# Initialise a storage
# Path to a database is optional. 'fsm_starage.db' will be created for default.
my_storage = SQLStorage('my_db_path.db')

# Initialize dispetcher with the storage
dp = Dispatcher(storage = my_storage)
```