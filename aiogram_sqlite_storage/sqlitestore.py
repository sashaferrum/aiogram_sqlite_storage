from __future__ import annotations

from aiogram.fsm.storage.base import BaseStorage, StorageKey
from aiogram.fsm.state import State
from typing import Any, Dict, Optional

import sqlite3
import pickle
import json

import logging
logger = logging.getLogger(__name__)


class SQLStorage(BaseStorage):
    
    def __init__(self, db_path: str = 'fsm_starage.db', serializing_method: str = 'pickle') -> None:
        """
        You can point a database path. It will be 'fsm_storage.db' for default.
        It's possible to choose srtializing method: 'pickle' (default) or 'json'. If you hange serializing method, you shoud delete existing database, and start a new one.
        'Pickle' is slower than 'json', but it can serialize some kind of objects, that 'json' cannot. 'Pickle' creates unreadable for human data in database, instead of 'json'.
        """
        self.db_path = db_path
        self.ser_m = serializing_method

        if self.ser_m != 'pickle' and self.ser_m != 'json':
            logger.warning(f"'{self.ser_m}' is unknown serializing method! A 'pickle' will be used.")
            self.ser_m = 'pickle'

        try:
            self.con = sqlite3.connect(self.db_path)
            self.con.execute("CREATE TABLE IF NOT EXISTS fsm_data (key TEXT PRIMARY KEY, state TEXT, data TEXT)")
            self.con.commit()
            logger.debug(f'FSM Storage database {self.db_path} has been opened.')
        except sqlite3.Error as e:
            logger.error(f'FSM Storage database opening error: {e}')


    def _key(self, key:StorageKey) -> str:
        """
        Create a key for every uniqe user, chat and bot
        """
        result_string = str(key.bot_id) + ':' + str(key.chat_id) + ':' + str(key.user_id)
        return result_string


    def _ser(self, obj: object) -> str|bytes|None:
        """
        Serialize object
        """
        try:
            if self.ser_m == 'json':
                return json.dumps(obj)
            else:
                return pickle.dumps(obj)
        except Exception as e:
            logger.error(f'Serializing error! {e}')
            return None


    def _dsr(self, obj) -> Optional[Dict[str, Any]]:
        """
        Deserialize object
        """
        try:
            if self.ser_m == 'json':
                return json.loads(obj)
            else:
                return pickle.loads(obj)
        except Exception as e:
            logger.error(f'Deserializing error! Probably, unsupported serializing method was used. {e}')
            return None
    

    async def set_state(self, key: StorageKey, state: State|None = None) -> None:
        """
        Set state for specified key

        :param key: storage key
        :param state: new state
        """
        s_key = self._key(key)
        s_state = state.state if isinstance(state, State) else state

        try:
            self.con.execute("INSERT OR REPLACE INTO fsm_data (key, state, data) VALUES (?, ?, COALESCE((SELECT data FROM fsm_data WHERE key = ?), NULL));",
                        (s_key, s_state, s_key))
            self.con.commit()
        except sqlite3.Error as e:
            logger.error(f'FSM Storage database error: {e}')


    async def get_state(self, key: StorageKey) -> Optional[str]:
        """
        Get key state

        :param key: storage key
        :return: current state
        """
        s_key = self._key(key)

        try:
            s_state = self.con.execute("SELECT state FROM fsm_data WHERE key = ?", (s_key,)).fetchone()
            
            if s_state and len(s_state) > 0:
                return s_state[0]
            else:
                return None
            
        except sqlite3.Error as e:
            logger.error(f'FSM Storage database error: {e}')
            return None


    
    async def set_data(self, key: StorageKey, data: Dict[str, Any]) -> None:
        """
        Write data (replace)

        :param key: storage key
        :param data: new data
        """
        s_key = self._key(key)
        s_data = self._ser(data)

        try:
            self.con.execute("INSERT OR REPLACE INTO fsm_data (key, state, data) VALUES (?, COALESCE((SELECT state FROM fsm_data WHERE key = ?), NULL), ?);",
                        (s_key, s_key, s_data))
            self.con.commit()
        except sqlite3.Error as e:
            logger.error(f'FSM Storage database error: {e}')

    
    async def get_data(self, key: StorageKey) -> Optional[Dict[str, Any]]:
        """
        Get current data for key

        :param key: storage key
        :return: current data
        """
        s_key = self._key(key)

        try:
            s_data = self.con.execute("SELECT data FROM fsm_data WHERE key = ?", (s_key,)).fetchone()
            
            if s_data and len(s_data) > 0:
                return self._dsr(s_data[0])
            else:
                return None

        except sqlite3.Error as e:
            logger.error(f'FSM Storage database error: {e}')
            return None


    async def update_data(self, key: StorageKey, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update date in the storage for key (like dict.update)

        :param key: storage key
        :param data: partial data
        :return: new data
        """
        current_data = await self.get_data(key=key)
        if not current_data:
            current_data = {}
        current_data.update(data)
        await self.set_data(key=key, data=current_data)
        return current_data.copy()


    
    async def close(self) -> None:  # pragma: no cover
        """
        Close storage (database connection, file or etc.)
        """
        self.con.close()
        logger.debug(f'FSM Storage database {self.db_path} has been closed.')
