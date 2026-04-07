import Foundation
import Testing
import Logging
@testable import LoggingSQLite

// Bootstrap once for all tests

let logDirectory: URL = {
    let sharedTempDir = FileManager.default.temporaryDirectory
        .appending(path: "swift-log-sqlite-tests-\(UUID().uuidString)", directoryHint: .isDirectory)
    
    print("Log directory located at \(sharedTempDir.path(percentEncoded: false))")
    LoggingSystem.bootstrap( { label in SQLiteLogHandler(label: label, logsDirectory: sharedTempDir) })
    
    return sharedTempDir
}()

@Suite(.serialized)
struct SwiftLogSQLiteTests
{
    let logDir: URL
    
    init() throws {
        self.logDir = logDirectory
    }
    
    @Test func ensureLoggerHasValueSemantics() throws
    {
        var logger1 = Logger(label: "first logger")
        logger1.logLevel = .debug
        logger1[metadataKey: "only-on"] = "first"
        
        var logger2 = logger1
        logger2.logLevel = .error                  // Must not affect logger1
        logger2[metadataKey: "only-on"] = "second" // Must not affect logger1
        
        // These expectations must pass
        #expect(logger1.logLevel == .debug)
        #expect(logger2.logLevel == .error)
        #expect(logger1[metadataKey: "only-on"] == "first")
        #expect(logger2[metadataKey: "only-on"] == "second")
    }
    
    @Test func testConcurrentWrites() throws
    {
        let logger = Logger(label: "Concurrent")
        
        let iterations = 10_000
        let msgPrefix = "Concurrent log message #"

        DispatchQueue.concurrentPerform(iterations: iterations) { i in
            logger.log(level: .info, "\(msgPrefix)\(i)")
        }
        
        // get the underlying handler
        
        #expect(logger.handler is SQLiteLogHandler)
        
        guard let sqliteLogHandler = logger.handler as? SQLiteLogHandler
        else { return }
        
        // --- Close and open as readonly
        
        let sqlStore = sqliteLogHandler.store
        sqlStore.close()
        
        let readonlyStore = try sqlStore.readonlyInstance()
        
        // --- Make sure we have all entries stored
        
        let entries = readonlyStore.readLogEntries(after: .distantPast)
        #expect(entries.count == iterations)
        
        // --- Make sure the highest counter == (iterations-1)
        
        var maxCounter = 0
        
        for entry in entries
        {
            let msg = entry.message
            let counterStart = msg.index(msg.startIndex, offsetBy: msgPrefix.count)
            let counter = Int(msg[counterStart...]) ?? 0
            maxCounter = max(counter, maxCounter)
        }
        
        #expect(maxCounter == (iterations-1))
    }
}

