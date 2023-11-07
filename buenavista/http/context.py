import logging
import queue
import uuid
from collections import defaultdict
from typing import Any, Dict, Optional
import requests
import re
import os
from fastapi import Request
import sqlglot

from ..core import Connection, Session, QueryResult

logger = logging.getLogger(__name__)

BD_CAT = "CREATE TABLE IF NOT EXISTS __bd_prepared_statements (key VARCHAR, st VARCHAR)"
BD_URL = "http://boilingdata_http_gw:3100/"


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

    ## Initial setup, create inmem boilingdata database and fetch Boiling catalog into it
    ## - DuckDB in-mem table is used by all connections, so we get Boiling Catalog only once
    def populate_boiling_catalog(self, txn_id):
        qr = self._sess.execute_sql("SHOW DATABASES;")
        for db in qr.rows():
            if db[0] == "boilingdata":
                return
        try:
            self._sess.execute_sql("ATTACH ':memory:' AS boilingdata;")
            self._sess.execute_sql("SET search_path='memory,boilingdata';")
        except:
            None
        try:
            print("---------------- BOILING POPULATE -----------------")
            # We could stick to the information_schema API but for now Boiling
            # handles the "CREATE TABLE" statements on server side for you.
            q = "SELECT * FROM information_schema.create_tables_statements"
            data = {"statement": q, "requestId": txn_id}
            response = requests.post(BD_URL, json=data)
            stmts = response.json()
            for stmt in stmts:
                print(f"\t{stmt}")
                self._sess.execute_sql(stmt)
        except:
            None

    ## information_schema
    def misc_sql_mangling(self, sql):
        sql = sql.replace("CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP")
        sql = sql.replace(":%S.%f%z", ":%S.%g%z")  # Fix timestamp format for Metabase
        sql = sql.replace(
            "WHERE catalog_name = '\"boilingdata\"'",
            "WHERE catalog_name = 'boilingdata'",
        )
        sql = sql.replace('\'"boilingdata".', "'")
        sql = re.sub(
            r"WHERE table_schema = '\"(.+?)\"'",
            r"WHERE table_schema = '\1'",
            sql,
        )
        # hide internal table
        t = "SELECT DISTINCT table_name as Table from information_schema.tables WHERE table_schema ="
        if t in sql:
            sql = f"{sql} AND table_name NOT LIKE '__bd_%'"
        # use information schema: DESCRIBE --> SELECT
        sql = re.sub(
            r"DESCRIBE \"(.+?)\"\.\"(.+?)\"\.\"(.+?)\"",
            r"SELECT column_name, data_type AS column_type, is_nullable AS null, '' AS key, column_default AS default, NULL AS extra FROM information_schema.columns WHERE table_catalog = '\1' AND table_schema = '\2' AND table_name = '\3';",
            sql,
        )
        sql = sql.replace(
            "SHOW SCHEMAS FROM", "SELECT * FROM information_schema.schemata"
        )
        sql = sql.replace(" USING ", " ")
        return sql

    def is_boiling_deallocate(self, sql):
        pattern = r"DEALLOCATE\s+PREPARE\s+([a-zA-Z0-9]+)\s*"
        match = re.search(pattern, sql, re.DOTALL)
        stmt = match.group(1) if match else "notResolved"
        if stmt != "notResolved":
            q = f"{BD_CAT}; DELETE FROM __bd_prepared_statements WHERE key = '{stmt}';"
            self._sess.execute_sql(q)
            return True
        return False

    def is_boiling_execute(self, sql):
        pattern = r"EXECUTE\s+([a-zA-Z0-9\._]+)\s+USING\s+"
        match = re.search(pattern, sql, re.DOTALL)
        stmtName = match.group(1) if match else "notResolved"
        if stmtName == "notResolved":
            return None
        q = f"{BD_CAT}; SELECT * FROM __bd_prepared_statements"
        bd_prepared_stmts = self._sess.execute_sql(q).rows()
        logger.debug(f"---------------- PREPARED LIST ({stmtName})-----------------")
        for stmt in bd_prepared_stmts:
            if stmt[0] == stmtName:
                arguments = re.sub(pattern, "", sql, flags=re.DOTALL).split(",")
                _sql = stmt[1]
                for arg in arguments:
                    _sql = _sql.replace("?", arg, 1)
                logger.debug(f"\t{_sql}")
                return _sql
        return None

    def is_boiling_intercept(self, sql):
        logger.debug("---------------- is_boiling_intercept? -----------------")
        ## 0) Is prepared statement for Boiling?
        if self.is_boiling_deallocate(sql):
            return False
        prepared = self.is_boiling_execute(sql)
        if prepared is not None:
            # print("\t YES")
            return prepared
        ## 1) Get all Boiling tables so we know what to intercept
        q = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_catalog = 'boilingdata';"
        boiling_tables = self._sess.execute_sql(q).rows()
        for table in boiling_tables:
            # print(f"\t{table[0]}.{table[1]}")
            if (
                sql
                and table[0] in sql
                and table[1] in sql
                and "SELECT column_name, data_type AS column_type, is_nullable AS null"
                not in sql
            ):
                ## Store prepared statements for our own use
                if "PREPARE " in sql:
                    sqlstr = re.sub(r"(?<!')'(?!')", "''", sql)
                    pattern = r"PREPARE\s+([a-zA-Z0-9\._]+)\s+(FROM|AS)\s+"
                    match = re.search(pattern, sql, re.DOTALL)
                    stmtName = match.group(1) if match else "notResolved"
                    logger.debug("---------------- NEW PREPARED -----------------")
                    q = f"{BD_CAT}; INSERT INTO __bd_prepared_statements VALUES ('{stmtName}', '{sqlstr}');"
                    logger.debug(q)
                    self._sess.execute_sql(q)
                    # print("\t NO")
                    return "SELECT 1;"  # nop
                # print("\t YES")
                return True
        ## 2) static words
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
        __sql = sql.lower().replace('"', "")
        if (
            not sql.lower().startswith("prepare ")
            and not "information_schema" in sql.lower()
            and any(term in __sql for term in boiling_search_terms)
        ):
            # print("\t YES")
            return True
        # print("\t NO")
        return False

    def execute_sql(self, sql: str) -> QueryResult:
        logger.debug(f"TXN %s: %s", self.txn_id, sql)
        txn_id = str(uuid.uuid4())
        self.populate_boiling_catalog(txn_id)
        sql = self.misc_sql_mangling(sql)
        resp_filename = None
        is_bd = self.is_boiling_intercept(sql)
        if isinstance(is_bd, str):
            sql = is_bd
        if is_bd and is_bd != "SELECT 1;":
            # Send the SQL to BD HTTP local proxy on port 3100
            # Boiling speaks PostgreSQL for now
            sql = sqlglot.transpile(sql, read="duckdb", write="postgres")[0]
            sql = sql.replace("pg_catalog.", "")
            pattern = r"CREATE\s+TABLE\s+([a-zA-Z0-9\._]+)\s*AS\s*"
            match = re.search(pattern, sql, re.DOTALL)
            results_tbl = match.group(1) if match else "tmpResults"
            sql = re.sub(pattern, "", sql, flags=re.DOTALL)
            logger.debug("---------------- BoilingData Query -----------------")
            response = requests.post(
                BD_URL, json={"statement": sql, "requestId": txn_id}
            )
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
        os.unlink(resp_filename) if resp_filename != None else None
        return qr

    def close(self):
        self.pool.release(self._sess, self.txn_id)
        self._sess = None

    def session(self) -> Session:
        return self._sess

    def headers(self) -> Dict:
        return self.h.write
