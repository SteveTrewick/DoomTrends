// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "DoomTrends",
    platforms: [
        .macOS(.v13)
    ],
    products: [
        .library(
            name: "DoomTrends",
            targets: ["DoomTrends"]
        )
    ],
    dependencies: [
        .package(url: "https://github.com/SteveTrewick/DoomModels.git", from: "0.1.0")
    ],
    targets: [
        .target(
            name: "DoomTrends",
            dependencies: [
                .product(name: "DoomModels", package: "DoomModels")
            ]
        ),
        .testTarget(
            name: "DoomTrendsTests",
            dependencies: ["DoomTrends"]
        )
    ]
)
