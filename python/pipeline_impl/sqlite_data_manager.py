import os
import re
from pathlib import Path
import sqlite3
import json

try:
    import lifeblood_connection
    in_lifeblood_runtime = True
except ImportError:
    in_lifeblood_runtime = False

from lifeblood_client.query import Task
from lifeblood_client.submitting import NewTask, EnvironmentResolverArguments

from pipeline.asset_data import AssetVersionData, AssetData, DataState
from pipeline.data_access_interface import DataAccessInterface, NotFoundError
from pipeline.future import ConditionCheckerFuture, FutureResult

from typing import Iterable, Tuple, List, Union


class LifebloodTaskFuture(ConditionCheckerFuture):

    def _check(self):
        task = self.__task
        return task.state == Task.TaskState.DONE and task.paused \
               or task.state == Task.TaskState.ERROR

    def _get_result(self):
        """
        just return True on success, False on Failure
        we expect lifeblood graph to be responsible for data setting to DB
        """
        return self._check() and self.__task.state != Task.TaskState.ERROR

    def __init__(self, addr, task_id):
        self.__task = Task(addr, task_id)
        super(LifebloodTaskFuture, self).__init__(self._check,
                                                  self._get_result)


class SqliteDataManagerWithLifeblood(DataAccessInterface):
    def __init__(self, db_path: Union[Path, str], lb_path: Tuple[str, int]):
        if isinstance(db_path, str):
            db_path = Path(db_path)
        self.__db_path = db_path
        self.__lb_addr = lb_path
        with sqlite3.connect(db_path) as con:
            con.executescript(_init_script)

    def get_asset_type_name(self, asset_path_id: str):
        with sqlite3.connect(self.__db_path) as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute(f'SELECT type_name FROM assets WHERE pathid == ?', asset_path_id)
            type_name = cur.fetchone()
            if type_name is None:
                raise NotFoundError(asset_path_id)
        return type_name

    def get_asset_datas(self, asset_path_ids: Iterable[str]) -> List[AssetData]:
        with sqlite3.connect(self.__db_path) as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            asset_path_ids = tuple(asset_path_ids)
            cur.execute(f'SELECT * FROM assets WHERE pathid IN ({",".join("?"*len(asset_path_ids))})', asset_path_ids)
            datas = cur.fetchall()
        ret = []
        for data in datas:
            assdata = AssetData(path_id=data['pathid'],
                                name=data['name'],
                                description=data['description'])
            ret.append(assdata)
        return ret

    def get_asset_version_datas(self, asset_path_id_version_pairs: Iterable[Tuple[str, Tuple[int, int, int]]]) -> List[AssetVersionData]:
        with sqlite3.connect(self.__db_path) as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            datas = []
            for pid, v in asset_path_id_version_pairs:
                print(pid, v)
                cur.execute('SELECT "pathid", "asset_pathid", version_0, version_1, version_2, data_task_attr, data_produced, data_calculator_id, data '
                            'FROM asset_versions WHERE asset_pathid == ? AND version_0 == ? AND version_1 == ? AND version_2 == ?', (pid, *v))
                datas.extend(cur.fetchall())
        ret = []
        for data in datas:
            assdata = AssetVersionData(path_id=data['pathid'],
                                       asset_path_id=data['asset_pathid'],
                                       version_id=(data['version_0'], data['version_1'], data['version_2']),
                                       data_producer_task_attrs=json.loads(data['data_task_attr']),
                                       data_availability=DataState(data['data_produced']),
                                       data_calculator_id=data['data_calculator_id'],
                                       data=json.loads(data['data']) if data['data'] is not None else None)
            ret.append(assdata)
        return ret

    def publish_new_asset_version(self, asset_path_id: str, version_data: AssetVersionData, dependencies: Iterable[str]):
        """
        if version_data.pathid is None - it will be assigned automatically based on asset_path_id and version_id
        if version_id is None - next available version_id will be assigned automatically

        """
        with sqlite3.connect(self.__db_path) as con:
            cur = con.cursor()
            cur.execute('SELECT pathid FROM assets WHERE pathid == ?', (asset_path_id,))
            if cur.fetchone() is None:
                raise RuntimeError('bad asset_path_id')
            cur.execute('BEGIN IMMEDIATE')

            if version_data.version_id is None:
                cur.execute('SELECT version_0, version_1, version_2 FROM asset_versions ORDER BY version_0 DESC, version_1 DESC, version_2 DESC LIMIT 1')
                ver = list(cur.fetchone() or [0, -1, -1])
                bump_idx = max(0, ver.index(-1)-1) if -1 in ver else 2
                ver[bump_idx] += 1
                version_data.version_id = tuple(ver)
            else:  # otherwise ensure version_id is legal
                cur.execute('SELECT pathid FROM asset_versions WHERE version_0 == ? AND version_1 == ? AND version_2 == ?', version_data.version_id)
                if cur.fetchone() is not None:
                    con.rollback()
                    raise RuntimeError(f'version_id "{version_data.version_id}" is already published')

            version_string = '.'.join(str(x) for x in version_data.version_id)
            pathid = version_data.path_id or f'{asset_path_id}/{version_string}'
            cur.execute('INSERT INTO asset_versions ("pathid", "asset_pathid", version_0, version_1, version_2, "data_task_attr") '
                        'VALUES (?, ?, ?,?,?, ?)',
                        (pathid,
                         asset_path_id,
                         *version_data.version_id,
                         json.dumps(version_data.data_producer_task_attrs)))

            if dependencies:
                cur.executemany('INSERT INTO asset_version_dependencies (dependant, depends_on) VALUES (?, ?)',
                                ((pathid, dep) for dep in dependencies))

            # update version_data fields
            version_data.path_id = pathid
            version_data.asset_path_id = asset_path_id
            version_data.data_availability = DataState.NOT_COMPUTED
            version_data.data_calculator_id = None
            version_data.data = None
            con.commit()
        return version_data

    def create_new_asset(self, asset_data: AssetData) -> AssetData:
        pathid = asset_data.path_id or re.sub(r'\W', '_', asset_data.name)
        with sqlite3.connect(self.__db_path) as con:
            cur = con.cursor()
            cur.execute('INSERT INTO assets ("pathid", "name", "description") VALUES (?, ?, ?)',
                        (pathid,
                        asset_data.name,
                        asset_data.description))
            asset_data.path_id = pathid
            con.commit()
        return asset_data

    def schedule_data_computation_for_asset_version(self, path_id) -> FutureResult:
        with sqlite3.connect(self.__db_path) as con:
            con.row_factory = sqlite3.Row
            con.execute('BEGIN IMMEDIATE')  # we start transaction here already to ensure consistency
            cur = con.cursor()
            cur.execute('SELECT data_produced, data_task_attr, data_calculator_id FROM asset_versions WHERE pathid == ?', (path_id,))
            data = cur.fetchone()
            if data is None:
                con.rollback()
                raise ValueError('path id "{}" does not exist'.format(path_id))
            state = DataState(data['data_produced'])

            if state == DataState.IS_COMPUTING:
                fut = LifebloodTaskFuture(self.__lb_addr, data['data_calculator_id'])
                con.rollback()
                return fut

            # schedule data coputation
            task_stuff = json.loads(data['data_task_attr'])
            if in_lifeblood_runtime:
                lifeblood_connection.create_task(task_stuff['name'], task_stuff['attribs'], blocking=True)
            else:
                task = NewTask(name=task_stuff.get('name', 'just some unnamed task'),
                               node_id=task_stuff.get('node_id', 2),  # 2 here is what defined by lifeblood network setup, the node has id=2, just so happened
                               scheduler_addr=self.__lb_addr,
                               env_args=EnvironmentResolverArguments(task_stuff.get('env', {}).get('name', 'StandardEnvironmentResolver'), task_stuff.get('env', {}).get('attribs', {})),
                               task_attributes=task_stuff.get('attribs', {}),
                               priority=task_stuff.get('priority', 50)).submit()

            cur.execute('UPDATE asset_versions SET data_produced = ?, data_calculator_id = ? WHERE pathid == ?', (DataState.IS_COMPUTING.value,
                                                                                                                  task.id,
                                                                                                                  path_id))
            con.commit()
            return LifebloodTaskFuture(self.__lb_addr, task.id)

    def _data_computation_completed_callback(self, path_id: str, data: dict):
        with sqlite3.connect(self.__db_path) as con:
            cur = con.cursor()
            cur.execute('BEGIN IMMEDIATE')
            cur.execute('SELECT data_produced FROM asset_versions WHERE pathid == ?', (path_id,))
            check_data = cur.fetchone()
            if check_data is None or check_data[0] != DataState.IS_COMPUTING.value:
                con.rollback()
                raise RuntimeError('data computation was not started, inconsistency!')
            cur.execute('UPDATE asset_versions SET data_produced = ?, data_calculator_id = ?, data = ? '
                        'WHERE pathid == ?', (DataState.AVAILABLE.value,
                                              -1,
                                              json.dumps(data),
                                              path_id)
                        )
            con.commit()

    # dependencies
    def get_version_dependencies(self, version_path_id: str) -> Iterable[str]:
        with sqlite3.connect(self.__db_path) as con:
            cur = con.cursor()
            cur.execute('SELECT depends_on FROM asset_version_dependencies WHERE dependant == ?', (version_path_id,))
            return [x[0] for x in cur.fetchall()]

    def get_dependent_versions(self, version_path_id: str) -> Iterable[str]:
        with sqlite3.connect(self.__db_path) as con:
            cur = con.cursor()
            cur.execute('SELECT dependant FROM asset_version_dependencies WHERE depends_on == ?', (version_path_id,))
            return [x[0] for x in cur.fetchall()]

    def add_dependencies(self, version_path_id: str, dependency_path_ids: Iterable[str]):
        if not dependency_path_ids:
            return
        with sqlite3.connect(self.__db_path) as con:
            cur = con.cursor()
            cur.executemany('INSERT OR IGNORE INTO asset_version_dependencies (dependant, depends_on) VALUES (?, ?)',
                            ((version_path_id, dep) for dep in dependency_path_ids))
            con.commit()

    def remove_dependencies(self, version_path_id: str, dependency_path_ids: Iterable[str]):
        if not dependency_path_ids:
            return
        with sqlite3.connect(self.__db_path) as con:
            cur = con.cursor()
            cur.executemany('DELETE FROM asset_version_dependencies WHERE dependant == ? AND depends_on == ?',
                            ((version_path_id, dep) for dep in dependency_path_ids))
            con.commit()


_init_script = \
'''
BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS "asset_versions" (
    "pathid"    TEXT NOT NULL UNIQUE,
    "asset_pathid"    TEXT NOT NULL,
    "version_0"    INTEGER NOT NULL DEFAULT 0,
    "version_1"    INTEGER NOT NULL DEFAULT -1,
    "version_2"    INTEGER NOT NULL DEFAULT -1,
    "data_task_attr"    TEXT,
    "data_produced"    INTEGER NOT NULL DEFAULT 0,
    "data_calculator_id"    INTEGER DEFAULT NULL,
    "data"    TEXT,
    FOREIGN KEY("asset_pathid") REFERENCES "assets"("pathid") ON UPDATE CASCADE ON DELETE CASCADE,
    PRIMARY KEY("pathid")
);
CREATE TABLE IF NOT EXISTS "assets" (
    "pathid"    TEXT NOT NULL UNIQUE,
    "name"    TEXT,
    "description"   TEXT,
    type_name       TEXT,
    PRIMARY KEY("pathid")
);
CREATE TABLE IF NOT EXISTS "asset_version_dependencies" (
    "dependant"    TEXT NOT NULL,
    "depends_on"   TEXT NOT NULL,
    FOREIGN KEY("dependant") REFERENCES "asset_versions"("pathid") ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY("depends_on") REFERENCES "asset_versions"("pathid") ON UPDATE CASCADE ON DELETE RESTRICT,
    UNIQUE(dependant,depends_on)
);
COMMIT;
PRAGMA journal_mode=wal;
PRAGMA synchronous=NORMAL;
'''