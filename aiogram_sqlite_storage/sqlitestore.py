from aiogram.fsm.storage.base import BaseStorage, StorageKey
from aiogram.fsm.state import State
from typing import Any, Dict, Optional

import sqlite3
import pickle

import logging
logger = logging.getLogger(__name__)


class SQLStorage(BaseStorage):
    
    def __init__(self, db_path: str = 'fsm_starage.db') -> None:
        """
        You can point a database path. It will be 'fsm_storage.db' for default.
        """
        self.db_path = db_path
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
        s = str(key.bot_id) + ':' + str(key.chat_id) + ':' + str(key.user_id)
        return s


    def _ser(self, arg) -> str:
        """
        Serialize object
        """
        return pickle.dumps(arg)


    def _dsr(self, s:str):
        """
        Deserialize object
        """
        return pickle.loads(s)
    

    async def set_state(self, key: StorageKey, state: State = None) -> None:
        """
        Set state for specified key

        :param key: storage key
        :param state: new state
        """
        s_key = self._key(key)
        s_state = self._ser(state)

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
            s_state = self.con.execute("SELECT state FROM fsm_data WHERE key = ?", (s_key,)).fetchone()[0]
            
            if s_state:
                return self._dsr(s_state)
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

    
    async def get_data(self, key: StorageKey) -> Dict[str, Any]:
        """
        Get current data for key

        :param key: storage key
        :return: current data
        """
        s_key = self._key(key)

        try:
            s_data = self.con.execute("SELECT data FROM fsm_data WHERE key = ?", (s_key,)).fetchone()[0]
            
            if s_data:
                return self._dsr(s_data)
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