#!/usr/bin/python3

import argparse
import sqlite3
import bumddb

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument ("output", help="Database to hold results", type = str)
    parser.add_argument ("inputs", help="Databases to integrate", type = str, nargs="+")
    args = parser.parse_args()

    destDB = sqlite3.connect(args.output)

    statusTable = bumddb.StatusTable(destDB, create = True)
    hostTable = bumddb.HostTable(destDB, create = True)
    fileshaTable = bumddb.FileshaTable(destDB, create = True)
    filepathTable = bumddb.FilepathTable(destDB, create = True)
    runTable = bumddb.RunTable(destDB, create = True)
    dirTable = bumddb.DirectoryTable(destDB, create = True)
    linkTable = bumddb.LinkTable(destDB, create = True)
    fileTable = bumddb.FileTable(destDB, create = True)

    for sourceDBPath in args.inputs:
        sourceDB = sqlite3.connect(sourceDBPath)

        counterCursor = sourceDB.cursor()

        counterCursor.execute("SELECT COUNT(0) FROM run_v1", [])
        runCount = counterCursor.fetchone()[0]

        runNumber = 0
        runCursor = sourceDB.cursor()
        runCursor.execute("SELECT r.id, h.host, r.starttime, r.endtime, s.status FROM run_v1 r JOIN host_v1 h ON r.host_id = h.id JOIN status_v1 s ON r.status_id = s.id;")
        for runResult in runCursor:
            runNumber += 1
            print ("Run", runNumber, "of", runCount)

            (sourceRunId, sourceHost, sourceStarttime, sourceEndtime, sourceStatus) = runResult
            
            counterCursor.execute("SELECT COUNT(0) FROM directory_v1 WHERE run_id = ?", (sourceRunId,))
            dirCount = counterCursor.fetchone()[0]
            print (" -", dirCount, "directories")

            counterCursor.execute("SELECT COUNT(0) FROM link_v1 WHERE run_id = ?", (sourceRunId,))
            linkCount = counterCursor.fetchone()[0]
            print (" -", linkCount, "symbolic links")

            counterCursor.execute("SELECT COUNT(0) FROM file_v1 WHERE run_id = ?", (sourceRunId,))
            fileCount = counterCursor.fetchone()[0]
            print (" -", fileCount, "files")

            destRunId = runTable.getId(sourceHost, sourceStarttime)
            runTable.updateStatus(destRunId, sourceStatus)
            runTable.updateEndtime(destRunId, sourceEndtime)

            dirNumber = 0

            dirCursor = sourceDB.cursor()
            dirCursor.execute("SELECT p.filepath, f.fileowner, f.filegroup, f.filemode, f.filetime FROM directory_v1 f JOIN filepath_v1 p ON f.filepath_id = p.id WHERE f.run_id = ?", (sourceRunId,))
            for dirResult in dirCursor:
                dirNumber += 1
                if (dirNumber % 1000 == 0):
                    print ("HOST", sourceHost, "RUN", runNumber, "of", runCount, "DIR ", dirNumber, "of", dirCount)

                (filePath, fileOwner, fileGroup, fileMode, fileTime) = dirResult

                dirTable.getId(destRunId, filePath, fileOwner, fileGroup, fileMode, fileTime)

            print ("HOST", sourceHost, "RUN", runNumber, "of", runCount, "DIR ", dirNumber, "of", dirCount)

            linkNumber = 0

            linkCursor = sourceDB.cursor()
            linkCursor.execute("SELECT s.filepath, d.filepath FROM link_v1 l JOIN filepath_v1 s ON l.filepath_id = s.id JOIN filepath_v1 d ON l.destpath_id = d.id WHERE l.run_id = ?", (sourceRunId,))
            for linkResult in linkCursor:
                linkNumber += 1
                if (linkNumber % 1000 == 0):
                    print ("HOST", sourceHost, "RUN", runNumber, "of", runCount, "LINK", linkNumber, "of", linkCount)

                (filePath, destPath) = linkResult

                linkTable.getId(destRunId, filePath, destPath)

            print ("HOST", sourceHost, "RUN", runNumber, "of", runCount, "LINK", linkNumber, "of", linkCount)

            fileNumber = 0
            
            fileCursor = sourceDB.cursor()
            fileCursor.execute("SELECT p.filepath, f.fileowner, f.filegroup, f.filemode, f.filesize, f.filetime, s.filesha FROM file_v1 f JOIN filepath_v1 p ON p.id = f.filepath_id JOIN filesha_v1 s ON s.id = f.filesha_id WHERE f.run_id = ?", (sourceRunId,))
            for fileResult in fileCursor:
                fileNumber += 1
                if (fileNumber % 1000 ==0):
                    print ("HOST", sourceHost, "RUN", runNumber, "of", runCount, "FILE", fileNumber, "of", fileCount)

                (filePath, fileOwner, fileGroup, fileMode, fileSize, fileTime, fileSha) = fileResult

                fileTable.getId(destRunId, filePath, fileOwner, fileGroup, fileMode, fileSize, fileTime, fileSha)

            print ("HOST", sourceHost, "RUN", runNumber, "of", runCount, "FILE", fileNumber, "of", fileCount)
            destDB.commit()
        
        sourceDB.close()
    
main()
