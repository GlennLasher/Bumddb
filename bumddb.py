#!/usr/bin/python3

import sqlite3
import time

class Table:
    """Implements a generic table and some methods to operate on one.
    These will be inherited by other classes.

    The basic concept is that you create a subclass of this class,
    then define the actual SQL statements in place of the ones defined
    below.  The dataSize variable defines the number of variables that
    should be expected by the getId method

    """

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
        """Sets up a Table object.  Put a reference to the database handle and
        the read-only flag on the object as parameters.  If warranted,
        drop and/or create the table by calling the relevant methods.

        """
        self.dbh = dbh
        self.readOnly = readOnly

        if (reset):
            create = True
            self.dropTable()

        if (create):
            self.createTable()

    def getId (self, *data):
        """Gets an ID for a value, come hell or high water.  If there is a
        record that matches the data, the ID of that record is
        returned.  If not, and readOnly is False, then it will insert
        a new record with the given data and then return the ID of the
        new record.  If readOnly is True and the record is not found,
        returns None.
        """
        
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
        """Creates the table and anything that needs to go with it by stepping
        through the commands stored in the createTable_list variable
        above or in a class that inherits this method.

        """
        for command in self.createTable_list:
            self.dbh.execute(command)

    def dropTable (self):
        """Drops the table and anything that goes with it by stepping through
        the commands stored in the dropTable_list variable above or in
        a class that inherits this method.

        """
        for command in self.dropTable_list:
            self.dbh.execute(command)

class StatusTable (Table):
    """Implements a table to hold the status of a backup.  This is based
    entirely on methods inherited from Table.

    """

    dataSize = 1
    getId_select = "SELECT id FROM status_v1 WHERE status = ?"
    getId_insert = "INSERT INTO status_v1 (status) VALUES (?)"

    createTable_list = [
        "CREATE TABLE IF NOT EXISTS status_v1 (id INTEGER PRIMARY KEY AUTOINCREMENT, status TEXT)",
        "CREATE INDEX IF NOT EXISTS status_v1_idx ON status_v1(status)"
    ]

    dropTable_list = [
        "DROP INDEX IF EXISTS status_v1_idx",
        "DROP TABLE IF EXISTS status_v1"
    ]

class HostTable (Table):
    """Implements a table to hold the hostnames of the systems backed up.
    This is based entirely on methods inherited from Table.

    """
    dataSize = 1
    getId_select = "SELECT id FROM host_v1 WHERE host = ?"
    getId_insert = "INSERT INTO host_v1 (host) VALUES (?)"

    createTable_list = [
        "CREATE TABLE IF NOT EXISTS host_v1 (id INTEGER PRIMARY KEY AUTOINCREMENT, host TEXT)",
        "CREATE INDEX IF NOT EXISTS host_v1_idx ON host_v1(host)"
    ]
    
    dropTable_list = [
        "DROP INDEX IF EXISTS host_v1_idx",
        "DROP TABLE IF EXISTS host_v1"
    ]

class FileshaTable (Table):
    """Implements a table to hold the SHA256 hashes of the files backed
    up.  This is based entirely on methods inherited from Table.

    """
    dataSize = 1
    getId_select = "SELECT id FROM filesha_v1 WHERE filesha = ?"
    getId_insert = "INSERT INTO filesha_v1 (filesha) VALUES (?)"

    createTable_list = [
        "CREATE TABLE IF NOT EXISTS filesha_v1 (id INTEGER PRIMARY KEY AUTOINCREMENT, filesha TEXT)",
        "CREATE INDEX IF NOT EXISTS filesha_v1_idx ON filesha_v1(filesha)"
    ]

    dropTable_list = [
        "DROP INDEX IF EXISTS filesha_v1_idx",
        "DROP TABLE IF EXISTS filesha_v1"
    ]
    
class FilepathTable (Table):
    """Implements a table to hold file metadata.  In addition to the
    primitives found in Table, this implements some methods to perform
    searches and reports.

    """
    dataSize = 1
    getId_select = "SELECT id FROM filepath_v1 WHERE filepath = ?"
    getId_insert = "INSERT INTO filepath_v1 (filepath) VALUES (?)"

    createTable_list = [
        "CREATE TABLE IF NOT EXISTS filepath_v1 (id INTEGER PRIMARY KEY AUTOINCREMENT, filepath TEXT)",
        "CREATE INDEX IF NOT EXISTS filepath_v1_idx ON filepath_v1(filepath)"
    ]

    dropTable_list = [
        "DROP INDEX IF EXISTS filepath_v1_idx",
        "DROP TABLE IF EXISTS filepath_v1"
    ]

    search_dir  = "SELECT DISTINCT 'DIR', h.host, f.filetime, p.filepath FROM host_v1 h, directory_v1 f, filepath_v1 p, run_v1 r WHERE p.filepath LIKE ? AND h.id = r.host_id AND r.id = f.run_id AND p.id = f.filepath_id ORDER BY f.filetime"
    search_link = "SELECT DISTINCT 'LINK', h.host, 0, p.filepath FROM host_v1 h, link_v1 f, filepath_v1 p, run_v1 r WHERE p.filepath LIKE ? AND h.id = r.host_id AND r.id = f.run_id AND p.id = f.filepath_id"
    search_file = "SELECT DISTINCT 'FILE', h.host, f.filetime, p.filepath FROM host_v1 h, file_v1 f, filepath_v1 p, run_v1 r WHERE p.filepath LIKE ? AND h.id = r.host_id AND r.id = f.run_id AND p.id = f.filepath_id ORDER BY f.filetime"

    def search(self, subjectlist):
        """Perform a substring search on the paths.

        """
        cursor = self.dbh.cursor()

        for term in subjectlist:
            for search in [self.search_dir, self.search_link, self.search_file]:
                cursor.execute(search, (("%" + term + "%"),))
                for result in cursor:
                    yield {'type'     : result[0],
                           'host'     : result[1],
                           'filetime' : result[2],
                           'filepath' : result[3]}

                

            
    
class RunTable (Table):
    """Implements a table to contain the characteristics and state of a
    backup that is being run.

    Note that even though this table contains more than two
    parameters, dataSize is set to 2 because only the node name and
    start time are known at the time that the record is inserted.
    Methods to alter the other two fields (end time and status) are
    provided.

    """
    dataSize = 2

    getId_select = "SELECT id FROM run_v1 WHERE host_id = ? AND starttime = ?"
    getId_insert = "INSERT INTO run_v1 (host_id, starttime) values (?, ?)"

    createTable_list = [
        "CREATE TABLE IF NOT EXISTS run_v1 (id INTEGER PRIMARY KEY AUTOINCREMENT, host_id INTEGER REFERENCES host(host_id), starttime INTEGER, endtime INTEGER, status_id INTEGER REFERENCES status(id))",
        "CREATE INDEX IF NOT EXISTS run_v1_idx ON run_v1 (host_id, starttime, endtime)",
    ]

    dropTable_list = [
        "DROP INDEX IF EXISTS run_v1_idx",
        "DROP TABLE IF EXISTS run_v1"
    ]

    updateStatus_update = "UPDATE run_v1 SET status_id = ? WHERE id = ?"

    updateEndtime_update = "UPDATE run_v1 SET endtime = ? WHERE id = ?"

    listBackups_nohost = "SELECT r.id, h.host, r.starttime, r.endtime, s.status FROM run_v1 r, host_v1 h, status_v1 s WHERE r.endtime >= ? AND r.starttime <= ? AND h.id = r.host_id AND s.id = r.status_id ORDER BY r.starttime"

    listBackups_withhost = "SELECT r.id, h.host, r.starttime, r.endtime, s.status FROM run_v1 r, host_v1 h, status_v1 s WHERE h.host = ? AND r.endtime >= ? AND r.starttime <= ? AND h.id = r.host_id AND s.id = r.status_id ORDER BY r.starttime"
    
    def __init__(self, dbh, readOnly = False, create = False, reset = False):
        """Initializes the RunTable object.  This differs from the generic
        Table type because it also needs an instance of StatusTable
        and HostTable for reference.

        """

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
        """Implements a special case of getId that performs a lookup to get
        the hostId for the insertion.  It also ends by setting the
        status ID to point to a status of Setup.

        """
        
        if (len(data) != self.dataSize):
            raise TypeError("getId is expecting %d arguments and got %d." %(self.dataSize, len(data)))

        (host, timestamp) = data

        hostId = self.hostTable.getId(host)

        runId = super(RunTable, self).getId(hostId, timestamp)

        self.updateStatus(runId, "Setup")
        return runId
        
    def updateStatus (self, runId, status):
        """Implements the means to change the value of the status for a given
        run.

        """
        statusId = self.statusTable.getId(status)

        cursor = self.dbh.cursor()
        cursor.execute(self.updateStatus_update, (statusId, runId))

    def updateEndtime (self, runId, endTime):
        """Implements the means to set the end time when a run finishes, dies
        or is terminated.

        """
        cursor = self.dbh.cursor()
        cursor.execute(self.updateEndtime_update, (endTime, runId))

    def listBackups (self, host = None, notBefore = None, notAfter = None):
        """Reports out a list of backup runs that match the given criteria.

        """
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
    """Implements a table to contain information about what directories
    exist in a backup.  Since directories are fungible, we just need
    to capture the path, permissions and timestamp to be able to
    create a new one that is functionally equivalent.

    """
    dataSize = 6

    getId_select = "SELECT id FROM directory_v1 WHERE run_id = ? AND filepath_id = ? AND fileowner = ? AND filegroup = ? AND filemode = ? AND filetime = ?"
    getId_insert = "INSERT INTO directory_v1 (run_id, filepath_id, fileowner, filegroup, filemode, filetime) VALUES (?, ?, ?, ?, ?, ?)"

    createTable_list = [
        "CREATE TABLE IF NOT EXISTS directory_v1 (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER REFERENCES run(id), filepath_id INTEGER REFERENCES filepath(id), fileowner INTEGER, filegroup INTEGER, filemode INTEGER, filetime INTEGER)",
        "CREATE INDEX IF NOT EXISTS directory_v1_idx ON directory_v1(filepath_id, fileowner, filegroup, filemode, filetime)"
    ]

    dropTable_list = [
        "DROP INDEX IF EXISTS directory_v1_idx",
        "DROP TABLE IF EXISTS directory_v1"
    ]

    restoreList_select_all = "SELECT p.filepath, d.fileowner, d.filegroup, d.filemode, d.filetime FROM directory_v1 d JOIN filepath_v1 p ON d.filepath_id = p.id WHERE d.run_id = ?"
    
    restoreList_select_subject = "SELECT p.filepath, d.fileowner, d.filegroup, d.filemode, d.filetime FROM directory_v1 d JOIN filepath_v1 p ON d.filepath_id = p.id WHERE d.run_id = ? AND p.filepath LIKE ?||'%'"
    
    def __init__(self, dbh, readOnly = False, create = False, reset = False):
        """Sets up the DirectoryTable object.  In addition to the basics, this
        instantiates a FilePathTable and puts it on the DirectoryTable
        object for reference use.

        """
        self.dbh = dbh
        self.readOnly = readOnly
        self.filepathTable = FilepathTable(dbh, readOnly)

        if (reset):
            create = True
            self.dropTable()

        if (create):
            self.createTable()

    def getId(self, *data):
        """Implements getId using the inherited version, but only after using
        the attached FilepathTable object to get a filepath ID.

        """
        if (len(data) != self.dataSize):
            raise TypeError("getId is expecting %d arguments and got %d." %(self.dataSize, len(data)))

        (runId, filepath, fileowner, filegroup, filemode, filetime) = data

        filepathId = self.filepathTable.getId(filepath)

        return super(DirectoryTable, self).getId(runId, filepathId, fileowner, filegroup, filemode, filetime)

    def restoreList(self, runId, subjectlist):
        """Reports out directories that need to be created during restore
        operations, along with their permissions and timestamps.  If
        subjectlist is empty, all directories under that runId are
        yielded.

        """
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
            for subject in subjectlist:
                cursor.execute(self.restoreList_select_subject, (runId, subject))
                for result in cursor:
                    yield {'filepath'  : result[0],
                           'fileowner' : result[1],
                           'filegroup' : result[2],
                           'filemode'  : result[3],
                           'filetime'  : result[4]}
                    
        
    
class LinkTable (Table):
    """Implements a table to contain information about symbolic links.
    Symlinks only have two data points: their location and the
    location they point to.  As such, this table will simply link two
    records out of the filepath table.

    It is worth noting that the destination path will not be massaged
    into an absolute path as will happen in other places in this
    system.  The reason for this is that a symlink may be relative,
    and if it is, it needs to be left that way so that a restoration
    in a different location may still stand a fighting chance.

    """
    #ID, Run Id, Filepath ID, Destpath ID

    dataSize = 3

    getId_select = "SELECT id FROM link_v1 WHERE run_id = ? AND filepath_id = ? AND destpath_id = ?"
    getId_insert = "INSERT INTO link_v1 (run_id, filepath_id, destpath_id) VALUES (?, ?, ?)"

    createTable_list = [
        "CREATE TABLE IF NOT EXISTS link_v1 (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER REFERENCES run(id), filepath_id INTEGER REFERENCES filepath(id), destpath_id INTEGER REFERENCES filepath(id))",
        "CREATE INDEX IF NOT EXISTS link_v1_idx ON link_v1(run_id)"
    ]


    dropTable_list = [
        "DROP INDEX IF EXISTS link_v1_idx",
        "DROP TABLE IF EXISTS link_v1"
    ]

    restoreList_select_all = "SELECT s.filepath, d.filepath FROM link_v1 l JOIN filepath_v1 s ON l.filepath_id = s.id JOIN filepath_v1 d ON l.destpath_id = d.id WHERE l.run_id = ?"
    
    restoreList_select_subject = "SELECT s.filepath, d.filepath FROM link_v1 l JOIN filepath_v1 s ON l.filepath_id = s.id JOIN filepath_v1 d ON l.destpath_id = d.id WHERE l.run_id = ? AND s.filepath LIKE ?||'%'"
    
    def __init__(self, dbh, readOnly = False, create = False, reset = False):
        """Sets up the LinkTable object.  As with other filesystem objects,
        this is being overridden so that we can put a FilepathTable
        object on this object for reference purposes.

        """
        self.dbh = dbh
        self.readOnly = readOnly
        self.filepathTable = FilepathTable(dbh, readOnly)

        if (reset):
            create = True
            self.dropTable()

        if (create):
            self.createTable()



    def getId(self, *data):
        """Overrides the getId from the Table class by starting with a pair of
        calls to FilepathTable.getId so that we have the IDs to
        insert.

        """
        if (len(data) != self.dataSize):
            raise TypeError("getId is expecting %d arguments and got %d." %(self.dataSize, len(data)))

        (runId, filepath, destpath) = data

        filepathId = self.filepathTable.getId(filepath)
        destpathId = self.filepathTable.getId(destpath)

        return super(LinkTable, self).getId(runId, filepathId, destpathId)

    def restoreList(self, runId, subjectlist):
        """Reports out symbolic links that need to be created during restore
        operation.  If subjectlist is empty, all directories under
        that runId are yielded.

        """
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
    """Implements a table to contain information about what files exist in
    a backup.  We capture the path, permissions, timestamp and SHA256
    hash for identification of the content.

    """
    #ID, run_id, filepath_id, fileowner, filegroup, filemode, filesize, filetime, filesha_id

    dataSize = 8

    getId_select = "SELECT id FROM file_v1 WHERE run_id = ? and filepath_id = ? and fileowner = ? and filegroup = ? and filemode = ? and filesize = ? and filetime = ? and filesha_id = ?"
    getId_insert = "INSERT INTO file_v1 (run_id, filepath_id, fileowner, filegroup, filemode, filesize, filetime, filesha_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"

    createTable_list = [
        "CREATE TABLE IF NOT EXISTS file_v1 (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER REFERENCES run(id), filepath_id INTEGER REFERENCES filepath(id), fileowner INTEGER, filegroup INTEGER, filemode INTEGER, filesize INTEGER, filetime INTEGER, filesha_id INTEGER REFERENCES filesha(id))",
        "CREATE INDEX IF NOT EXISTS file_v1_idx ON file_v1(filepath_id, filesize, filetime, run_id, fileowner, filegroup, filemode, filesha_id)"
    ]
    
    dropTable_list = [
        "DROP INDEX IF EXISTS file_v1_idx",
        "DROP TABLE IF EXISTS file_v1"
    ]

    getExistingRecord_select = "SELECT s.filesha FROM filesha_v1 s, file_v1 f, run_v1 r WHERE r.host_id = ? AND f.filepath_id = ? AND f.filesize = ? AND f.filetime = ? AND f.run_id = r.id AND s.id = f.filesha_id ORDER BY r.starttime DESC LIMIT 1"

    restoreList_select_all = "SELECT p.filepath, f.fileowner, f.filegroup, f.filemode, f.filetime, s.filesha FROM file_v1 f JOIN filepath_v1 p ON p.id = f.filepath_id JOIN filesha_v1 s ON s.id = f.filesha_id WHERE f.run_id = ?"

    restoreList_select_subject = "SELECT p.filepath, f.fileowner, f.filegroup, f.filemode, f.filetime, s.filesha FROM file_v1 f JOIN filepath_v1 p ON p.id = f.filepath_id JOIN filesha_v1 s ON s.id = f.filesha_id WHERE f.run_id = ? AND f.filepath LIKE ?||'%'"
    
    
    def __init__(self, dbh, readOnly = False, create = False, reset = False):
        """Sets up the FileTable object.  In addition to the basics, this
        instantiates a FilepathTable, FileshaTable and HostTable
        object and puts them on the FileTable object for reference
        use.

        """
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
        """Implements getId using the inherited version, but only after using
        the attached FilepathTable and FileshaTable to get the
        relevant IDs to insert/search for.

        """
        if (len(data) != self.dataSize):
            raise TypeError("getId is expecting %d arguments and got %d." %(self.dataSize, len(data)))

        (runId, filepath, fileowner, filegroup, filemode, filesize, filetime, filesha) = data

        filepathId = self.filepathTable.getId(filepath)
        fileshaId = self.fileshaTable.getId(filesha)

        return super(FileTable, self).getId(runId, filepathId, fileowner, filegroup, filemode, filesize, filetime, fileshaId)

    def getExistingRecord(self, host, filepath, filesize, filetime):
        """Looks to see if there is a record that matches on host, path, size
        and timestamp, and returns it.  This is for fast-mode backups,
        in which the hashing step is skipped on the assumption that
        the environment is not hostile.

        """
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
        """Reports out files that need to be created during restore
        operations, along with their permissions, timestamps and
        hashes.  If subjectlist is empty, all files under that runId
        are yielded.

        """
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
