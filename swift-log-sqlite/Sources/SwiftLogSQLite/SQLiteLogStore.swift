//
//  SQLiteLogStore.swift
//  swift-log-sqlite
//
//  Created by Matthias Maurberger on 02.10.25.
//

import Foundation
import SQLite3
import Logging

public struct SQLError: Swift.Error {
    /// The [error code](https://www.sqlite.org/c3ref/c_abort.html).
    public let code: Int32

    /// The [error message](https://www.sqlite.org/c3ref/errcode.html).
    public var message: String

    init?(code: Int32, db: OpaquePointer) {
        guard !(code == SQLITE_ROW || code == SQLITE_OK || code == SQLITE_DONE) else { return nil }

        self.code = code
        self.message = String(cString: sqlite3_errmsg(db))
    }

    public init(code: Int32, message: String) {
        self.code = code
        self.message = message
    }
}

public class SQLiteLogStore: @unchecked Sendable
{
    static let LogBundleExtension = "logstore"

    private var db: OpaquePointer!
    private var insertStatement: OpaquePointer!
    
    private let queue: DispatchQueue
    private var isClosing: Bool = false
    
    public let fileUrl: URL
    
    private init(storeFile: URL) throws
    {
        self.fileUrl = storeFile
        
        queue = DispatchQueue(label: "sqlite-store", qos: .utility)
        
        try queue.sync {
            // NOTE(mcm): Since all of our db access happens on our serial dispatch queue, we open
            // the SQLite connection itself in Mutli-Threaded mode (SQLite_OPEN_NOMUTEX)
            try sqlCall {
                sqlite3_open_v2(storeFile.path, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, nil)
            }

            try sqlCall {
                sqlite3_exec(db, LogEntry.Schema, nil, nil, nil)
            }
            
            /// Setup WAL-mode
            ///
            /// https://www.sqlite.org/pragma.html#pragma_synchronous
            /// > The synchronous=NORMAL setting provides the best balance between performance and
            /// > safety for most applications running in WAL mode.
            
            try sqlCall {
                sqlite3_exec(db, "PRAGMA journal_mode=WAL;", nil, nil, nil)
            }
            
            try sqlCall {
                sqlite3_exec(db, "PRAGMA synchronous=NORMAL;", nil, nil, nil)
            }
            
            try sqlCall {
                sqlite3_prepare_v2(db, LogEntry.Insert, -1, &insertStatement, nil)
            }
        }
    }
    
    public init(readonly file: URL) throws
    {
        self.fileUrl = file
        queue = DispatchQueue(label: "sqlite-store", qos: .userInitiated)
        
        try queue.sync {
            _ = try sqlCall {
                sqlite3_open_v2(file.path, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, nil)
                sqlite3_exec(db, "PRAGMA journal_mode=DELETE;", nil, nil, nil)
                sqlite3_close_v2(db)
                return sqlite3_open_v2(file.path, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX, nil)
            }
        }
        
    }
    
    public func close() {
        queue.sync {
            guard !isClosing else { return }
            isClosing = true
            
            if let stmt = insertStatement { sqlite3_finalize(stmt) }
            if let db = db { sqlite3_close_v2(db) }
        }
    }
    
    deinit {
        close()
    }
    
    @discardableResult
    func sqlCall(_ body: () -> Int32) throws -> Int32 {
        let result = body()
        
        if let error = SQLError(code: result, db: db) {
            throw error
        }
        return result
    }
    
    @discardableResult
    func isOK(_ code: Int32) throws -> Int32 {
        guard let error = SQLError(code: code, db: db) else { return code }
        throw error
    }
    
    public convenience init(logsDirectory: URL)
    {
        let now = Date()
        let formattedTime = now.formatted(
            .iso8601.year().month().day().dateSeparator(.omitted).time(includingFractionalSeconds: false).timeSeparator(.omitted).timeZone(separator: .omitted)
        )
        let storeBundleName = "\(formattedTime).\(Self.LogBundleExtension)"
        
        let storeBundleUrl = logsDirectory.appendingPathComponent(storeBundleName, isDirectory: true)
        
        try! FileManager.default.createDirectory(at: storeBundleUrl, withIntermediateDirectories: true)
        
        let storeDBUrl = storeBundleUrl.appendingPathComponent("db.sqlite", isDirectory: false)
        
        try! self.init(storeFile: storeDBUrl)
    }
    
    func log(_ entry: LogEntry)
    {
        queue.async {
            guard !self.isClosing,
                  let stmt = self.insertStatement
            else {
                return print("⚠️ ","Skip message '\(entry.message)'")
            }
            
            sqlite3_reset(stmt)
            sqlite3_clear_bindings(stmt)

            sqlite3_bind_double(stmt, 1, entry.timestamp.timeIntervalSince1970)
            
            let result = entry.category.withCString { (s) in
                sqlite3_bind_text(stmt, 2, s, -1, nil)
                return entry.level.withCString { (s) in
                    sqlite3_bind_text(stmt, 3, s, -1, nil)
                    return entry.message.withCString { (s) in
                        sqlite3_bind_text(stmt, 4, s, -1, nil)
                        return entry.source.withCString { (s) in
                            sqlite3_bind_text(stmt, 5, s, -1, nil)
                            return entry.location.withCString { (s) in
                                sqlite3_bind_text(stmt, 6, s, -1, nil)
                                if let metadata = entry.metadata {
                                    return metadata.withCString { (s) in
                                        sqlite3_bind_text(stmt, 7, s, -1, nil)
                                        return sqlite3_step(stmt)
                                    }
                                }
                                else {
                                    sqlite3_bind_null(stmt, 7)
                                    return sqlite3_step(stmt)
                                }
                            }
                        }
                    }
                }
            }
            
            if let error = SQLError(code: result, db: self.db)
            {
                print("⚠️ ", "Log message '\(entry.message)' prevented by \(error)")
            }
        }
    }
    
    func getRowCount() -> Int {
        queue.sync {
            var statement: OpaquePointer?
            defer { sqlite3_finalize(statement) }
            
            let query = "SELECT COUNT(*) FROM log_entry"
            guard sqlite3_prepare_v2(self.db, query, -1, &statement, nil) == SQLITE_OK,
                  sqlite3_step(statement) == SQLITE_ROW else {
                return 0
            }
            
            return Int(sqlite3_column_int(statement, 0))
        }
    }
    
    public func readLogEntries(after timestamp: Date, descending: Bool = false) -> [LogEntry]
    {
        queue.sync {
            var statement: OpaquePointer?
            defer { sqlite3_finalize(statement) }

            let order = descending ? "DESC" : "ASC"
            let query = """
                SELECT timestamp, category, level, message, source, location, metadata
                FROM log_entry
                WHERE timestamp > ?
                ORDER BY timestamp \(order)
                """

            guard sqlite3_prepare_v2(self.db, query, -1, &statement, nil) == SQLITE_OK else {
                return []
            }

            sqlite3_bind_double(statement, 1, timestamp.timeIntervalSince1970)

            var entries: [LogEntry] = []

            while sqlite3_step(statement) == SQLITE_ROW {
                let timestampValue = sqlite3_column_double(statement, 0)
                let category = String(cString: sqlite3_column_text(statement, 1))
                let level = String(cString: sqlite3_column_text(statement, 2))
                let message = String(cString: sqlite3_column_text(statement, 3))
                let source = String(cString: sqlite3_column_text(statement, 4))
                let location = String(cString: sqlite3_column_text(statement, 5))

                let metadata: String?
                if let metadataText = sqlite3_column_text(statement, 6) {
                    metadata = String(cString: metadataText)
                } else {
                    metadata = nil
                }

                let entry = LogEntry(
                    timestamp: Date(timeIntervalSince1970: timestampValue),
                    category: category,
                    level: level,
                    message: message,
                    source: source,
                    location: location,
                    metadata: metadata
                )

                entries.append(entry)
            }

            return entries
        }
    }
}

extension SQLiteLogStore
{
    func readonlyInstance() throws -> SQLiteLogStore
    {
        try SQLiteLogStore(readonly: self.fileUrl)
    }
}

public struct LogEntry
{
    static let TableName  = "log_entry"
    
    static let Schema = """
        CREATE TABLE IF NOT EXISTS \(TableName) (
            timestamp      DATETIME  NOT NULL,
            category       TEXT      NOT NULL,
            level          TEXT      NOT NULL,
            message        TEXT      NOT NULL,
            source         TEXT      NOT NULL,
            location       TEXT      NOT NULL,
            metadata       TEXT
        );
        """
    
    static let Insert = #"INSERT INTO \#(TableName) (timestamp, category, level, message, source, location, metadata) VALUES (?, ?, ?, ?, ?, ?, ?) "#
    
    /// Column `timestamp` (`DATETIME`), required (has default).
    public var timestamp : Date
    
    /// Column `category` (`TEXT`), required.
    public var category : String
    
    /// Column `level` (`TEXT`), required.
    public var level : String
    
    /// Column `message` (`TEXT`), required.
    public var message : String
    
    /// Column `source` (`TEXT`), required.
    public var source : String
    
    /// Column `location` (`TEXT`), required.
    public var location : String
    
    /// Column `metadata` (`TEXT`), optional (default: `nil`).
    public var metadata : String?
}

public struct SQLiteLogHandler: LogHandler
{
    public subscript(metadataKey key: String) -> Logger.Metadata.Value? {
        get { metadata[key] }
        set { metadata[key] = newValue }
    }
    
    public var metadata: Logger.Metadata = [:]
    
    public var logLevel: Logger.Level = .info
    
    let store: SQLiteLogStore
    let category: String
    
    public init(label: String, logsDirectory: URL)
    {
        self.category = label
        self.store = SQLiteLogStore(logsDirectory: logsDirectory)
    }
    
    public init(label: String, store: SQLiteLogStore)
    {
        self.category = label
        self.store = store
    }
    
    public func log(
        level: Logger.Level,
        message: Logger.Message,
        metadata: Logger.Metadata?,
        source: String,
        file: String,
        function: String,
        line: UInt
    ) {
        
        var effectiveMetadata = self.metadata
        if let metadata {
            effectiveMetadata.merge(metadata) { _, explicit in explicit }
        }
        
        let metaDataFlattened: String? = if effectiveMetadata.isEmpty {
            nil
        } else {
            effectiveMetadata.map({ (key, value) in "\(key)=\(value)" }).joined(separator: " ")
        }
        
        let entry = LogEntry(
            timestamp: Date.now,
            category: self.category,
            level: level.rawValue,
            message: message.description,
            source: source,
            location: "\(file): \(line) \(function)",
            metadata: metaDataFlattened
        )
        
        store.log(entry)
    }
}
