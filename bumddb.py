#!/usr/bin/python3

import sqlite3

class Table:
    dataSize = 1
    getId_select = "SELECT id FROM foo WHERE foo = ?"
    getId_insert = "INSERT INTO foo (foo) VALUES (?)"

    createTable_list = [
        "CREATE TABLE IF NOT EXISTS foo (id INTEGER PRIMARY KEY AUTOINCREMENT, foo TEXT)",
        "CREATE INDEX IF NOT EXISTS foo_idx ON foo(foo)"
    ]

    dropTable_list = [
        "DROP INDEX IF EXISTS foo_idx",
        "DROP TABLE IF EXISTS foo"
    ]

    def __init__(self, dbh, readOnly = False, create = False, reset = False):
        self.dbh = dbh
        self.readOnly = readOnly

        if (reset):
            create = True
            self.dropTable()

        if (create):
            self.createTable()

    def getId (self, *data):
        if (len(data) != self.dataSize):
            raise TypeError("getId is expecting %d arguments and got %d." %(self.dataSize, len(data)))

        cursor = self.dbh.cursor()
        rowId = None

        cursor.execute(self.getId_select, data)
        result = cursor.fetchone()

        if (self.readOnly and (result is None)):
            return None
        elif (result is None):
            cursor.execute(self.getId_insert, data)
            rowId = cursor.lastrowid
        else:
            rowId = result[0]

        return rowId

    def createTable (self):
        for command in self.createTable_list:
            self.dbh.execute(command)

    def dropTable (self):
        for command in self.dropTable_list:
            self.dbh.execute(command)

class StatusTable (Table):
    dataSize = 1
    getId_select = "SELECT id FROM status WHERE status = ?"
    getId_insert = "INSERT INTO status (status) VALUES (?)"

    createTable_list = [
        "CREATE TABLE IF NOT EXISTS status (id INTEGER PRIMARY KEY AUTOINCREMENT, status TEXT)",
        "CREATE INDEX IF NOT EXISTS status_idx ON status(status)"
    ]

    dropTable_list = [
        "DROP INDEX IF EXISTS status_idx",
        "DROP TABLE IF EXISTS status"
    ]

class HostTable (Table):
    dataSize = 1
    getId_select = "SELECT id FROM host WHERE host = ?"
    getId_insert = "INSERT INTO host (host) VALUES (?)"

    createTable_list = [
        "CREATE TABLE IF NOT EXISTS host (id INTEGER PRIMARY KEY AUTOINCREMENT, host TEXT)",
        "CREATE INDEX IF NOT EXISTS host_idx ON host(host)"
    ]
    
    dropTable_list = [
        "DROP INDEX IF EXISTS host_idx",
        "DROP TABLE IF EXISTS host"
    ]

class FileshaTable (Table):
    dataSize = 1
    getId_select = "SELECT id FROM filesha WHERE filesha = ?"
    getId_insert = "INSERT INTO filesha (filesha) VALUES (?)"

    createTable_list = [
        "CREATE TABLE IF NOT EXISTS filesha (id INTEGER PRIMARY KEY AUTOINCREMENT, filesha TEXT)",
        "CREATE INDEX IF NOT EXISTS filesha_idx ON filesha(filesha)"
    ]

    dropTable_list = [
        "DROP INDEX IF EXISTS filesha_idx",
        "DROP TABLE IF EXISTS filesha"
    ]
    
class FilepathTable (Table):
    dataSize = 1
    getId_select = "SELECT id FROM filepath WHERE filepath = ?"
    getId_insert = "INSERT INTO filepath (filepath) VALUES (?)"

    createTable_list = [
        "CREATE TABLE IF NOT EXISTS filepath (id INTEGER PRIMARY KEY AUTOINCREMENT, filepath TEXT)",
        "CREATE INDEX IF NOT EXISTS filepath_idx ON filepath(filepath)"
    ]

    dropTable_list = [
        "DROP INDEX IF EXISTS filepath_idx",
        "DROP TABLE IF EXISTS filepath"
    ]
    
class RunTable (Table):
    dataSize = 2

    getId_select = "SELECT id FROM run WHERE host_id = ? AND starttime = ?"
    getId_insert = "INSERT INTO run (host_id, starttime) values (?, ?)"

    createTable_list = [
        "CREATE TABLE IF NOT EXISTS run (id INTEGER PRIMARY KEY AUTOINCREMENT, host_id INTEGER REFERENCES host(host_id), starttime INTEGER, endtime INTEGER, status_id INTEGER REFERENCES status(id)",
        "CREATE INDEX IF NOT EXISTS run_idx ON run (host_id, starttime, endtime)",
    ]

    dropTable_list = [
        "DROP INDEX IF EXISTS run_idx",
        "DROP TABLE IF EXISTS run"
    ]

    updateStatus_update = "UPDATE run SET status_id = ? WHERE id = ?"

    updateEndtime_update = "UPDATE run SET endtime = ? WHERE id = ?"
    
    def __init__(self, dbh, readOnly = False):
        self.dbh = dbh
        self.readOnly = readOnly
        self.statusTable = StatusTable(dbh, readOnly)
        self.hostTable = StatusTable(dbh, readOnly)
    
    def updateStatus (self, runId, status):
        if (isinstance(status, int)):
            statusId = status
        else:
            statusId = statusTable.getId(status)

        cursor = self.dbh.cursor()
        cursor.execute(updateStatus_update, [statusId, runId])

    def updateEndtime (self, runId, endTime):
        cursor = self.dbh.cursor()
        cursor.execute(updateEndtime_update, [endTime, runId])

class DirectoryTable (Table):
    dataSize = 6

    getId_select = "SELECT id FROM directory WHERE run_id = ? AND filepath_id = ? AND fileowner = ? AND filegroup = ? AND filemode = ? AND filetime = ?"
    getId_insert = "INSERT INTO directory (run_id, filepath_id, fileowner, filegroup, filemode, filetime) VALUES (?, ?, ?, ?, ?, ?)"

    createTable_list = [
        "CREATE TABLE IF NOT EXISTS directory (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER REFERENCES run(id), filepath_id INTEGER REFERENCES filepath(id), fileowner INTEGER, filegroup INTEGER, filemode INTEGER, filetime INTEGER)"
    ]

    dropTable_list = [
        "DROP TABLE IF EXISTS directory"
    ]

    def __init__(self, dbh, readOnly = False):
        self.dbh = dbh
        self.readOnly = readOnly
        self.filepathTable = FilePathTable(dbh, readOnly)
    
    def getId(self, *data):
        if (len(data) != self.dataSize):
            raise TypeError("getId is expecting %d arguments and got %d." %(self.dataSize, len(data)))

        (runId, filepath, fileowner, filegroup, filemode, filetime) = data

        filepathId = self.filepathTable.getId(filepath)

        return super(DirectoryTable, self).getId(runId, filepathId, fileowner, filegroup, filemode, filetime)

class LinkTable (Table):

    #ID, Run Id, Filepath ID, Destpath ID

    dataSize = 3

    getId_select = "SELECT id FROM link WHERE run_id = ? AND filepath_id = ? AND destpath_id = ?"
    getId_insert = "INSERT INTO link (run_id, filepath_id, destpath_id_ values (?, ?, ?)"

    createTable_list = [
        "CREATE TABLE IF NOT EXISTS link (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER REFERENCES run(id), filepath_id INTEGER REFERENCES filepath(id), destpath_id INTEGER REFERENCES filepath(id))",
        "CREATE INDEX IF NOT EXISTS link_idx ON link(run_id)"
    ]


    dropTable_list = [
        "DROP INDEX IF EXISTS link_idx",
        "DROP TABLE IF EXISTS link"
    ]
    
    def __init__(self, dbh, readOnly = False):
        self.dbh = dbh
        self.readOnly = readOnly
        self.filepathTable = FilePathTable(dbh, readOnly)

    def getId(self, *data):
        if (len(data) != self.dataSize):
            raise TypeError("getId is expecting %d arguments and got %d." %(self.dataSize, len(data)))

        (runId, filepath, destpath) = data

        filepathId = self.filepathTable.getId(filepath)
        destpathId = self.filepathTable.getId(destpath)

        return super(LinkTable, self).getId(runId, filepathId, destpathId)

class FileTable (Table):
    #ID, run_id, filepath_id, fileowner, filegroup, filemode, filesize, filetime, filesha_id

    dataSize = 8

    getId_select = "SELECT id FROM file WHERE run_id = ? and filepath_id = ? and fileowner = ? and filegroup = ? and filemode = ? and filesize = ? and filetime = ? and filesha_id = ?"
    getId_insert = "INSERT INTO file (run_id, filepath_id, fileowner, filegroup, filemode, filesyze, filetime, filesha_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"

    createTable_list = [
        "CREATE TABLE IF NOT EXISTS file (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER REFERENCES run(id), filepath_id INTEGER REFERENCES filepath(id), fileowner INTEGER, filegroup INTEGER, filemode INTEGER, filesize INTEGER, filetime INTEGER, filesha_id INTEGER REFERENCES filesha(id)"
    ]
    
    dropTable_list = [
        "DROP TABLE IF EXISTS file"
    ]

    getExistingRecord_select = "SELECT s.filesha FROM filesha s, file f, run r WHERE r.host_id = ? AND f.filepath_id = ? AND f.filesize = ? AND f.run_id = r.id AND s.id = f.filesha_id ORDER BY run.starttime DESC LIMIT 1"

    def __init__(self, dbh, readOnly = False):
        self.dbh = dbh
        self.readOnly = readOnly
        self.filepathTable = FilepathTable(dbh, readOnly)
        self.fileshaTable = FileshaTable(dbh, readOnly)
        self.hostTable = HostTable(dbh, readOnly)

    def getId(self, *data):
        if (len(data) != self.dataSize):
            raise TypeError("getId is expecting %d arguments and got %d." %(self.dataSize, len(data)))

        (runId, filepath, fileowner, filegroup, filemode, filesize, filetime, filesha) = data

        filepathId = self.filepathTable.getId(filepath)
        fileshaId = self.fileshaTable.getId(filesha)

        return super(FileTable, self).getId(runId, filepathId, fileowner, filegroup, filemode, filesize, filetime, fileshaId)

    def getExistingRecord(self, host, filepath, filesize):
        hostId = self.hostTable(host)
        filepathId = self.filepathTable(filepath)

        cursor = self.dbh.cursor()
        cursor.execute(self.getExistingRecord_select, hostId, filepathId, filesize)
        result = fetchone()

        if (result is None):
            return None
        else:
            return (result[0])
        
