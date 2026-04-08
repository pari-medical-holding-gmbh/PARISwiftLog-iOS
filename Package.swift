// swift-tools-version: 6.2
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "PARILogger-iOS",
    platforms: [
        .iOS(.v16),
        .macOS(.v12),
    ],
    products: [
        .library(
            name: "LoggingSQLite",
            targets: ["LoggingSQLite"]
        ),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-log.git", from: "1.11.0"),
    ],
    targets: [
        .target(
            name: "LoggingSQLite",
            dependencies: [
                .product(name: "Logging", package: "swift-log"),
            ],
        ),
        .testTarget(
            name: "LoggingSQLiteTests",
            dependencies: ["LoggingSQLite"]
        ),
    ]
)
