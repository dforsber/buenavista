import logging
import queue
import uuid
from collections import defaultdict
from typing import Any, Dict, Optional
import requests
import json
import re
import os
from fastapi import Request

from ..core import Connection, Session, QueryResult

logger = logging.getLogger(__name__)


class Headers:
    def __init__(self, req: Request):
        self.read = req.headers
        self.write = {}

    def get(self, name: str, default: Optional[Any] = None) -> Optional[Any]:
        name = name.lower()
        return self.read.get(
            f"x-trino-{name}", self.read.get(f"x-presto-{name}", default)
        )

    def set(self, name: str, value: Any):
        name = name.lower()
        self.write[f"x-trino-{name}"] = value
        self.write[f"x-presto-{name}"] = value

    def clear(self, name: str):
        name = name.lower()
        del self.write[f"x-trino-{name}"]
        del self.write[f"x-presto-{name}"]


class SessionPool:
    def __init__(self):
        self.pool = queue.SimpleQueue()
        self.txns = {}

    def acquire(self, conn: Connection, txn_id: Optional[Any] = None) -> Session:
        if txn_id in self.txns:
            return self.txns[txn_id]
        else:
            try:
                sess = self.pool.get(block=False)
            except queue.Empty:
                sess = conn.create_session()
            return sess

    def release(self, sess: Session, txn_id: Optional[Any] = None):
        if txn_id:
            self.txns[txn_id] = sess
        else:
            self.pool.put(sess)


class Context:
    POOLS = defaultdict(SessionPool)

    def __init__(self, conn: Connection, req: Request):
        self.h = Headers(req)
        self.txn_id = self.h.get("Transaction-Id")
        if self.txn_id == "NONE":
            self.txn_id = None
        self.pool = self.POOLS[self.h.get("User", "default")]
        self._sess = self.pool.acquire(conn, self.txn_id)

        # Use a target catalog/schema, if specified
        use_target = None
        if catalog := self.h.get("Catalog"):
            use_target = catalog
        if schema := self.h.get("Schema"):
            if schema == "default":
                schema = "main"

            if use_target:
                use_target += f".{schema}"
            else:
                use_target = schema
        # if use_target:
        #     self._sess.execute_sql(f"USE {use_target}")

    def execute_sql(self, sql: str) -> QueryResult:
        logger.debug(f"TXN %s: %s", self.txn_id, sql)
        boiling_search_terms = [
            "'s3://",
            "glue('",
            "glue ('",
            "list('",
            "list ('",
            "share('",
            "share ('",
            "boilingdata",
            "boilingshares",
        ]
        txn_id = str(uuid.uuid4())
        resp_filename = ""
        if any(term in sql.lower() for term in boiling_search_terms):
            # Send the SQL to BD HTTP local proxy on port 3100
            logger.info("---------------- BoilingData Interception -----------------")
            pattern = r"CREATE\s*TABLE\s*([a-zA-Z0-9\._]+)\s*AS\s*"
            match = re.search(pattern, sql, re.DOTALL)
            results_tbl = match.group(1) if match else "tmpResults"
            sql = re.sub(pattern, "", sql, flags=re.DOTALL)
            logger.info("outgoing SQL\n", sql)
            url = "http://boilingdata_http_gw:3100/"
            response = requests.post(url, json={"statement": sql, "requestId": txn_id})
            resp_filename = f"/dev/shm/bd_resp_{txn_id}.json"
            with open(resp_filename, "wb") as outfile:
                outfile.write(response.content)
            self._sess.execute_sql(
                f"""
                DROP TABLE IF EXISTS tmpResults; 
                CREATE TABLE {results_tbl} AS SELECT * FROM read_json_auto('{resp_filename}')
                """
            )
            sql = f"SELECT * FROM {results_tbl}"
        qr = self._sess.execute_sql(sql)
        ends_in_txn = self._sess.in_transaction()
        logger.debug("FINISH IN TXN: %s", ends_in_txn)
        if not self.txn_id and ends_in_txn:
            self.txn_id = txn_id
            self.h.set("Started-Transaction-Id", self.txn_id)
            logger.debug("Set txn id to %s", self.txn_id)
        elif self.txn_id and not ends_in_txn:
            self.h.set("Clear-Transaction-Id", self.txn_id)
            logger.debug("Cleared transaction id from %s", self.txn_id)
            self.txn_id = None
        os.unlink(resp_filename) if resp_filename != "" else None
        return qr

    def close(self):
        self.pool.release(self._sess, self.txn_id)
        self._sess = None

    def session(self) -> Session:
        return self._sess

    def headers(self) -> Dict:
        return self.h.write
