#!/usr/bin/python3

import sqlite3
import time

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

    search_dir  = "SELECT DISTINCT 'DIR', h.host, f.filetime, p.filepath FROM host h, directory f, filepath p, run r WHERE p.filepath LIKE ? AND h.id = r.host_id AND r.id = f.run_id AND p.id = f.filepath_id ORDER BY f.filetime"
    search_link = "SELECT DISTINCT 'LINK', h.host, '---- -- -- -- -- --', p.filepath FROM host h, link f, filepath p, run r WHERE p.filepath LIKE ? AND h.id = r.host_id AND r.id = f.run_id AND p.id = f.filepath_id"
    search_file = "SELECT DISTINCT 'FILE', h.host, f.filetime, p.filepath FROM host h, file f, filepath p, run r WHERE p.filepath LIKE ? AND h.id = r.host_id AND r.id = f.run_id AND p.id = f.filepath_id ORDER BY f.filetime"

    def search(self, subjectlist):
        cursor = self.dbh.cursor()

        for term in subjectlist:
            for search in [self.search_dir, self.search_link, self.search_file]:
                cursor.execute(search, (("%" + term + "%"),))
                for result in cursor:
                    yield {'type'    : result[0],
                           'host'     : result[1],
                           'filetime' : result[2],
                           'filepath' : result[3]}

                

            
    
class RunTable (Table):
    dataSize = 2

    getId_select = "SELECT id FROM run WHERE host_id = ? AND starttime = ?"
    getId_insert = "INSERT INTO run (host_id, starttime) values (?, ?)"

    createTable_list = [
        "CREATE TABLE IF NOT EXISTS run (id INTEGER PRIMARY KEY AUTOINCREMENT, host_id INTEGER REFERENCES host(host_id), starttime INTEGER, endtime INTEGER, status_id INTEGER REFERENCES status(id))",
        "CREATE INDEX IF NOT EXISTS run_idx ON run (host_id, starttime, endtime)",
    ]

    dropTable_list = [
        "DROP INDEX IF EXISTS run_idx",
        "DROP TABLE IF EXISTS run"
    ]

    updateStatus_update = "UPDATE run SET status_id = ? WHERE id = ?"

    updateEndtime_update = "UPDATE run SET endtime = ? WHERE id = ?"

    listBackups_nohost = "SELECT r.id, h.host, r.starttime, r.endtime, s.status FROM run r, host h, status s WHERE r.endtime >= ? AND r.starttime <= ? AND h.id = r.host_id AND s.id = r.status_id ORDER BY r.starttime"

    listBackups_withhost = "SELECT r.id, h.host, r.starttime, r.endtime, s.status FROM run r, host h, status s WHERE h.host = ? AND r.endtime >= ? AND r.starttime <= ? AND h.id = r.host_id AND s.id = r.status_id ORDER BY r.starttime"
    
    def __init__(self, dbh, readOnly = False, create = False, reset = False):
        self.dbh = dbh
        self.readOnly = readOnly
        self.statusTable = StatusTable(dbh, readOnly)
        self.hostTable = HostTable(dbh, readOnly)

        if (reset):
            create = True
            self.dropTable()

        if (create):
            self.createTable()

    def getId (self, *data):
        if (len(data) != self.dataSize):
            raise TypeError("getId is expecting %d arguments and got %d." %(self.dataSize, len(data)))

        (host, timestamp) = data

        hostId = self.hostTable.getId(host)

        runId = super(RunTable, self).getId(hostId, timestamp)

        self.updateStatus(runId, "Setup")
        return runId
        
    def updateStatus (self, runId, status):
        statusId = self.statusTable.getId(status)

        cursor = self.dbh.cursor()
        cursor.execute(self.updateStatus_update, (statusId, runId))

    def updateEndtime (self, runId, endTime):
        cursor = self.dbh.cursor()
        cursor.execute(self.updateEndtime_update, (endTime, runId))

    def listBackups (self, host = None, notBefore = None, notAfter = None):
        cursor = self.dbh.cursor()

        if (notBefore is None):
            notBefore = 0

        if (notAfter is None):
            notAfter = time.time()
            
        if (host is None):
            cursor.execute(self.listBackups_nohost, (notBefore, notAfter))
        else:
            cursor.execute(self.listBackups_withhost, (host, notBefore, notAfter))

            
        for result in cursor:
            yield {"runId"     : result[0],
                   "host"      : result[1],
                   "starttime" : result[2],
                   "endtime"   : result[3],
                   "status"    : result[4]}
            
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

    restoreList_select_all = "SELECT p.filepath, d.fileowner, d.filegroup, d.filemode, d.filetime FROM directory d JOIN filepath p ON d.filepath_id = p.id WHERE d.run_id = ?"
    
    restoreList_select_subject = "SELECT p.filepath, d.fileowner, d.filegroup, d.filemode, d.filetime FROM directory d JOIN filepath p ON d.filepath_id = p.id WHERE d.run_id = ? AND p.filepath LIKE ?||'%'"
    
    def __init__(self, dbh, readOnly = False, create = False, reset = False):
        self.dbh = dbh
        self.readOnly = readOnly
        self.filepathTable = FilepathTable(dbh, readOnly)

        if (reset):
            create = True
            self.dropTable()

        if (create):
            self.createTable()

    def getId(self, *data):
        if (len(data) != self.dataSize):
            raise TypeError("getId is expecting %d arguments and got %d." %(self.dataSize, len(data)))

        (runId, filepath, fileowner, filegroup, filemode, filetime) = data

        filepathId = self.filepathTable.getId(filepath)

        return super(DirectoryTable, self).getId(runId, filepathId, fileowner, filegroup, filemode, filetime)

    def restoreList(self, runId, subjectlist):
        cursor = self.dbh.cursor()
        if (len(subjectlist) == 0):
            cursor.execute(self.restoreList_select_all, (runId,))
            for result in cursor:
                yield {'filepath'  : result[0],
                       'fileowner' : result[1],
                       'filegroup' : result[2],
                       'filemode'  : result[3],
                       'filetime'  : result[4]}
        else:
            for subject in subjects:
                cursor.execute(self.restoreList_select_subject, (runId, subject))
                for result in cursor:
                    yield {'filepath'  : result[0],
                           'fileowner' : result[1],
                           'filegroup' : result[2],
                           'filemode'  : result[3],
                           'filetime'  : result[4]}
                    
        
    
class LinkTable (Table):

    #ID, Run Id, Filepath ID, Destpath ID

    dataSize = 3

    getId_select = "SELECT id FROM link WHERE run_id = ? AND filepath_id = ? AND destpath_id = ?"
    getId_insert = "INSERT INTO link (run_id, filepath_id, destpath_id) VALUES (?, ?, ?)"

    createTable_list = [
        "CREATE TABLE IF NOT EXISTS link (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER REFERENCES run(id), filepath_id INTEGER REFERENCES filepath(id), destpath_id INTEGER REFERENCES filepath(id))",
        "CREATE INDEX IF NOT EXISTS link_idx ON link(run_id)"
    ]


    dropTable_list = [
        "DROP INDEX IF EXISTS link_idx",
        "DROP TABLE IF EXISTS link"
    ]

    restoreList_select_all = "SELECT s.filepath, d.filepath FROM link l JOIN filepath s ON l.filepath_id = s.id JOIN filepath d ON l.destpath_id = d.id WHERE l.run_id = ?"
    
    restoreList_select_subject = "SELECT s.filepath, d.filepath FROM link l JOIN filepath s ON l.filepath_id = s.id JOIN filepath d ON l.destpath_id = d.id WHERE l.run_id = ? AND s.filepath LIKE ?||'%'"
    
    def __init__(self, dbh, readOnly = False, create = False, reset = False):
        self.dbh = dbh
        self.readOnly = readOnly
        self.filepathTable = FilepathTable(dbh, readOnly)

        if (reset):
            create = True
            self.dropTable()

        if (create):
            self.createTable()



    def getId(self, *data):
        if (len(data) != self.dataSize):
            raise TypeError("getId is expecting %d arguments and got %d." %(self.dataSize, len(data)))

        (runId, filepath, destpath) = data

        filepathId = self.filepathTable.getId(filepath)
        destpathId = self.filepathTable.getId(destpath)

        return super(LinkTable, self).getId(runId, filepathId, destpathId)

    def restoreList(self, runId, subjectlist):
        cursor = self.dbh.cursor()
        if (len(subjectlist) == 0):
            cursor.execute(self.restoreList_select_all, (runId,))
            for result in cursor:
                yield {'filepath'  : result[0],
                       'destpath'  : result[1]}
        else:
            for subject in subjects:
                cursor.execute(self.restoreList_select_subject, (runId, subject))
                for result in cursor:
                    yield {'filepath'  : result[0],
                           'destpath'  : result[1]}
    
class FileTable (Table):
    #ID, run_id, filepath_id, fileowner, filegroup, filemode, filesize, filetime, filesha_id

    dataSize = 8

    getId_select = "SELECT id FROM file WHERE run_id = ? and filepath_id = ? and fileowner = ? and filegroup = ? and filemode = ? and filesize = ? and filetime = ? and filesha_id = ?"
    getId_insert = "INSERT INTO file (run_id, filepath_id, fileowner, filegroup, filemode, filesize, filetime, filesha_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"

    createTable_list = [
        "CREATE TABLE IF NOT EXISTS file (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER REFERENCES run(id), filepath_id INTEGER REFERENCES filepath(id), fileowner INTEGER, filegroup INTEGER, filemode INTEGER, filesize INTEGER, filetime INTEGER, filesha_id INTEGER REFERENCES filesha(id))"
    ]
    
    dropTable_list = [
        "DROP TABLE IF EXISTS file"
    ]

    getExistingRecord_select = "SELECT s.filesha FROM filesha s, file f, run r WHERE r.host_id = ? AND f.filepath_id = ? AND f.filesize = ? AND f.filetime = ? AND f.run_id = r.id AND s.id = f.filesha_id ORDER BY r.starttime DESC LIMIT 1"

    restoreList_select_all = "SELECT p.filepath, f.fileowner, f.filegroup, f.filemode, f.filetime, s.filesha FROM file f JOIN filepath p ON p.id = f.filepath_id JOIN filesha s ON s.id = f.filesha_id WHERE f.run_id = ?"

    restoreList_select_subject = "SELECT p.filepath, f.fileowner, f.filegroup, f.filemode, f.filetime, s.filesha FROM file f JOIN filepath p ON p.id = f.filepath_id JOIN filesha s ON s.id = f.filesha_id WHERE f.run_id = ? AND f.filepath LIKE ?||'%'"
    
    
    def __init__(self, dbh, readOnly = False, create = False, reset = False):
        self.dbh = dbh
        self.readOnly = readOnly
        self.filepathTable = FilepathTable(dbh, readOnly)
        self.fileshaTable = FileshaTable(dbh, readOnly)
        self.hostTable = HostTable(dbh, readOnly)

        if (reset):
            create = True
            self.dropTable()

        if (create):
            self.createTable()

    def getId(self, *data):
        if (len(data) != self.dataSize):
            raise TypeError("getId is expecting %d arguments and got %d." %(self.dataSize, len(data)))

        (runId, filepath, fileowner, filegroup, filemode, filesize, filetime, filesha) = data

        filepathId = self.filepathTable.getId(filepath)
        fileshaId = self.fileshaTable.getId(filesha)

        return super(FileTable, self).getId(runId, filepathId, fileowner, filegroup, filemode, filesize, filetime, fileshaId)

    def getExistingRecord(self, host, filepath, filesize, filetime):
        hostId = self.hostTable.getId(host)
        filepathId = self.filepathTable.getId(filepath)

        cursor = self.dbh.cursor()
        cursor.execute(self.getExistingRecord_select, (hostId, filepathId, filesize, filetime))
        result = cursor.fetchone()

        if (result is None):
            return None
        else:
            return (result[0])
        
    def restoreList(self, runId, subjectlist):
        cursor = self.dbh.cursor()
        if (len(subjectlist) == 0):
            cursor.execute(self.restoreList_select_all, (runId,))
            for result in cursor:
                yield {'filepath'  : result[0],
                       'fileowner' : result[1],
                       'filegroup' : result[2],
                       'filemode'  : result[3],
                       'filetime'  : result[4],
                       'filesha'   : result[5]}
        else:
            for subject in subjects:
                cursor.execute(self.restoreList_select_subject, (runId, subject))
                for result in cursor:
                    yield {'filepath'  : result[0],
                           'fileowner' : result[1],
                           'filegroup' : result[2],
                           'filemode'  : result[3],
                           'filetime'  : result[4],
                           'filesha'   : result[5]}
