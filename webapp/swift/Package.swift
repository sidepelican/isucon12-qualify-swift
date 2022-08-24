// swift-tools-version: 5.7

import PackageDescription

let package = Package(
    name: "isuports",
    platforms: [.macOS(.v13)],
    products: [
        .executable(name: "isuports", targets: ["isuports"])
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-tools-support-core.git", from: "0.3.0"),
        .package(url: "https://github.com/vapor/vapor.git", from: "4.65.1"),
        .package(url: "https://github.com/vapor/mysql-kit.git", from: "4.5.2"),
        .package(url: "https://github.com/vapor/sqlite-kit.git", from: "4.1.0"),
        .package(url: "https://github.com/vapor/jwt-kit.git", from: "4.7.0"),
        .package(url: "https://github.com/yaslab/CSV.swift", from: "2.4.3"),
    ],
    targets: [
        .executableTarget(
            name: "isuports",
            dependencies: [
                .product(name: "CSV", package: "CSV.swift"),
                .product(name: "JWTKit", package: "jwt-kit"),
                .product(name: "MySQLKit", package: "mysql-kit"),
                .product(name: "SQLiteKit", package: "sqlite-kit"),
                .product(name: "TSCBasic", package: "swift-tools-support-core"),
                .product(name: "Vapor", package: "vapor"),
            ],
            swiftSettings: [
                .unsafeFlags(["-strict-concurrency=complete"]),
            ]
        ),
    ]
)
