import Foundation
import XCTest
import DoomModels
@testable import DoomTrends

final class TrendDetectorTests: XCTestCase {
    func testTrendingDetectsBigramsAcrossSources() async {
        let config = TrendDetector.Configuration(
            shortWindow: 120,
            baselineWindow: 600,
            bucketSize: 60,
            enableBigrams: true,
            enableTrigrams: false,
            enableTitleCasePhrases: false,
            sampleHeadlineLimit: 2,
            minShortCount: 2,
            minUniqueSources: 1
        )
        let detector = TrendDetector(configuration: config)
        let now = Date()

        let itemA = NewsItem(
            feedID: "feed-a",
            source: "SourceA",
            title: "Oil prices surge on supply fears",
            body: nil,
            url: URL(string: "https://example.com/a")!,
            publishedAt: now,
            ingestedAt: now
        )
        let itemB = NewsItem(
            feedID: "feed-b",
            source: "SourceB",
            title: "Oil prices rise after outage",
            body: nil,
            url: URL(string: "https://example.com/b")!,
            publishedAt: now,
            ingestedAt: now
        )

        await detector.ingest([itemA, itemB])
        let topics = await detector.trending(now: now)

        XCTAssertTrue(topics.contains { $0.term == "oil prices" && $0.shortCount >= 2 })
    }

    func testAliasMapCanonicalizesTokens() async {
        let config = TrendDetector.Configuration(
            shortWindow: 300,
            baselineWindow: 900,
            bucketSize: 60,
            enableBigrams: false,
            enableTrigrams: false,
            enableTitleCasePhrases: false,
            stopwords: [],
            sampleHeadlineLimit: 1,
            minShortCount: 1,
            minUniqueSources: 1
        )
        let detector = TrendDetector(configuration: config)
        await detector.updateAliasMap([
            "fed": "federal reserve"
        ])
        let now = Date()

        let item = NewsItem(
            feedID: "feed-a",
            source: "SourceA",
            title: "Fed signals patience on rates",
            body: nil,
            url: URL(string: "https://example.com/fed")!,
            publishedAt: now,
            ingestedAt: now
        )

        await detector.ingest(item)
        let topics = await detector.trending(now: now)

        XCTAssertTrue(topics.contains { $0.term == "federal reserve" })
    }

    func testTitleCasePhrasesCapture() async {
        let config = TrendDetector.Configuration(
            shortWindow: 300,
            baselineWindow: 900,
            bucketSize: 60,
            enableBigrams: false,
            enableTrigrams: false,
            enableTitleCasePhrases: true,
            sampleHeadlineLimit: 1,
            minShortCount: 1,
            minUniqueSources: 1
        )
        let detector = TrendDetector(configuration: config)
        let now = Date()

        let item = NewsItem(
            feedID: "feed-a",
            source: "SourceA",
            title: "Federal Reserve Chair signals caution",
            body: nil,
            url: URL(string: "https://example.com/fed-chair")!,
            publishedAt: now,
            ingestedAt: now
        )

        await detector.ingest(item)
        let topics = await detector.trending(now: now)

        XCTAssertTrue(topics.contains { $0.term == "federal reserve" })
    }

    func testURLsAreIgnoredDuringExtraction() async {
        let config = TrendDetector.Configuration(
            shortWindow: 300,
            baselineWindow: 900,
            bucketSize: 60,
            enableBigrams: false,
            enableTrigrams: false,
            enableTitleCasePhrases: false,
            stopwords: [],
            sampleHeadlineLimit: 1,
            minShortCount: 1,
            minUniqueSources: 1
        )
        let detector = TrendDetector(configuration: config)
        let now = Date()

        let item = NewsItem(
            feedID: "feed-a",
            source: "SourceA",
            title: "https://example.com/foo?bar=baz",
            body: "www.example.org",
            url: URL(string: "https://example.com/foo")!,
            publishedAt: now,
            ingestedAt: now
        )

        await detector.ingest(item)
        let topics = await detector.trending(now: now)

        XCTAssertTrue(topics.isEmpty)
    }

    func testApostrophesDoNotSplitTokens() async {
        let config = TrendDetector.Configuration(
            shortWindow: 300,
            baselineWindow: 900,
            bucketSize: 60,
            enableBigrams: false,
            enableTrigrams: false,
            enableTitleCasePhrases: false,
            sampleHeadlineLimit: 1,
            minShortCount: 1,
            minUniqueSources: 1
        )
        let detector = TrendDetector(configuration: config)
        let now = Date()

        let item = NewsItem(
            feedID: "feed-a",
            source: "SourceA",
            title: "won\u{2019}t change",
            body: nil,
            url: URL(string: "https://example.com/wont")!,
            publishedAt: now,
            ingestedAt: now
        )

        await detector.ingest(item)
        let topics = await detector.trending(now: now)

        XCTAssertTrue(topics.contains { $0.term == "change" })
        XCTAssertFalse(topics.contains { $0.term == "won" })
    }

    func testShortProperNounsAndAcronymsAreKept() async {
        let config = TrendDetector.Configuration(
            shortWindow: 300,
            baselineWindow: 900,
            bucketSize: 60,
            enableBigrams: false,
            enableTrigrams: false,
            enableTitleCasePhrases: false,
            sampleHeadlineLimit: 1,
            minShortCount: 1,
            minUniqueSources: 1
        )
        let detector = TrendDetector(configuration: config)
        let now = Date()

        let item = NewsItem(
            feedID: "feed-a",
            source: "SourceA",
            title: "US Wu backed plan",
            body: nil,
            url: URL(string: "https://example.com/us-wu")!,
            publishedAt: now,
            ingestedAt: now
        )

        await detector.ingest(item)
        let topics = await detector.trending(now: now)

        XCTAssertTrue(topics.contains { $0.term == "US" })
        XCTAssertTrue(topics.contains { $0.term == "wu" })
    }

    func testTitleCaseWeightBoostsScore() async {
        let weights = TrendDetector.Configuration.Weights(
            accelWeight: 0,
            sourceWeight: 0,
            countLogWeight: 1.0,
            titleCaseWeight: 2.0,
            allCapsWeight: 1.0
        )
        let config = TrendDetector.Configuration(
            shortWindow: 300,
            baselineWindow: 900,
            bucketSize: 60,
            enableBigrams: false,
            enableTrigrams: false,
            enableTitleCasePhrases: false,
            stopwords: [],
            weights: weights,
            sampleHeadlineLimit: 1,
            minShortCount: 1,
            minUniqueSources: 1
        )
        let detector = TrendDetector(configuration: config)
        let now = Date()

        let trumpItem = NewsItem(
            feedID: "feed-a",
            source: "SourceA",
            title: "Trump visits",
            body: nil,
            url: URL(string: "https://example.com/trump")!,
            publishedAt: now,
            ingestedAt: now
        )
        let policyItem = NewsItem(
            feedID: "feed-b",
            source: "SourceB",
            title: "policy update",
            body: nil,
            url: URL(string: "https://example.com/policy")!,
            publishedAt: now,
            ingestedAt: now
        )

        await detector.ingest([trumpItem, policyItem])
        let topics = await detector.trending(now: now)

        let trumpScore = topics.first { $0.term == "trump" }?.score ?? 0
        let policyScore = topics.first { $0.term == "policy" }?.score ?? 0
        XCTAssertGreaterThan(trumpScore, policyScore)
    }

    func testAllCapsWeightBoostsScore() async {
        let weights = TrendDetector.Configuration.Weights(
            accelWeight: 0,
            sourceWeight: 0,
            countLogWeight: 1.0,
            titleCaseWeight: 1.0,
            allCapsWeight: 2.0
        )
        let config = TrendDetector.Configuration(
            shortWindow: 300,
            baselineWindow: 900,
            bucketSize: 60,
            enableBigrams: false,
            enableTrigrams: false,
            enableTitleCasePhrases: false,
            stopwords: [],
            weights: weights,
            sampleHeadlineLimit: 1,
            minShortCount: 1,
            minUniqueSources: 1
        )
        let detector = TrendDetector(configuration: config)
        let now = Date()

        let usItem = NewsItem(
            feedID: "feed-a",
            source: "SourceA",
            title: "US meets",
            body: nil,
            url: URL(string: "https://example.com/us")!,
            publishedAt: now,
            ingestedAt: now
        )
        let policyItem = NewsItem(
            feedID: "feed-b",
            source: "SourceB",
            title: "policy update",
            body: nil,
            url: URL(string: "https://example.com/policy-2")!,
            publishedAt: now,
            ingestedAt: now
        )

        await detector.ingest([usItem, policyItem])
        let topics = await detector.trending(now: now)

        let usScore = topics.first { $0.term == "US" }?.score ?? 0
        let policyScore = topics.first { $0.term == "policy" }?.score ?? 0
        XCTAssertGreaterThan(usScore, policyScore)
    }

    func testStopwordsDroppedUnlessAllCaps() async {
        let config = TrendDetector.Configuration(
            shortWindow: 300,
            baselineWindow: 900,
            bucketSize: 60,
            enableBigrams: false,
            enableTrigrams: false,
            enableTitleCasePhrases: false,
            stopwords: ["to"],
            sampleHeadlineLimit: 1,
            minShortCount: 1,
            minUniqueSources: 1
        )
        let detector = TrendDetector(configuration: config)
        let now = Date()

        let item = NewsItem(
            feedID: "feed-a",
            source: "SourceA",
            title: "To Alpha US",
            body: nil,
            url: URL(string: "https://example.com/to-alpha")!,
            publishedAt: now,
            ingestedAt: now
        )

        await detector.ingest(item)
        let topics = await detector.trending(now: now)

        XCTAssertFalse(topics.contains { $0.term == "to" })
        XCTAssertTrue(topics.contains { $0.term == "alpha" })
        XCTAssertTrue(topics.contains { $0.term == "US" })
    }

    func testStopwordUpdates() async {
        let config = TrendDetector.Configuration(
            shortWindow: 300,
            baselineWindow: 900,
            bucketSize: 60,
            enableBigrams: false,
            enableTrigrams: false,
            enableTitleCasePhrases: false,
            sampleHeadlineLimit: 1,
            minShortCount: 1,
            minUniqueSources: 1
        )
        let detector = TrendDetector(configuration: config)
        let now = Date()

        await detector.addStopwords(["customterm"])
        await detector.ingest(NewsItem(
            feedID: "feed-a",
            source: "SourceA",
            title: "CustomTerm appears",
            body: nil,
            url: URL(string: "https://example.com/custom")!,
            publishedAt: now,
            ingestedAt: now
        ))
        var topics = await detector.trending(now: now)
        XCTAssertFalse(topics.contains { $0.term == "customterm" })

        await detector.removeStopwords(["customterm"])
        await detector.ingest(NewsItem(
            feedID: "feed-a",
            source: "SourceA",
            title: "CustomTerm appears again",
            body: nil,
            url: URL(string: "https://example.com/custom2")!,
            publishedAt: now,
            ingestedAt: now
        ))
        topics = await detector.trending(now: now)
        XCTAssertTrue(topics.contains { $0.term == "customterm" })
    }

    func testPhraseStopwordsFilterBigrams() async {
        let config = TrendDetector.Configuration(
            shortWindow: 300,
            baselineWindow: 900,
            bucketSize: 60,
            enableBigrams: true,
            enableTrigrams: false,
            enableTitleCasePhrases: false,
            phraseStopwords: ["guard"],
            sampleHeadlineLimit: 1,
            minShortCount: 1,
            minUniqueSources: 1
        )
        let detector = TrendDetector(configuration: config)
        let now = Date()

        let item = NewsItem(
            feedID: "feed-a",
            source: "SourceA",
            title: "guard oil tanker",
            body: nil,
            url: URL(string: "https://example.com/guard")!,
            publishedAt: now,
            ingestedAt: now
        )

        await detector.ingest(item)
        let topics = await detector.trending(now: now)

        XCTAssertFalse(topics.contains { $0.term == "guard oil" })
        XCTAssertTrue(topics.contains { $0.term == "oil tanker" })
    }

    func testDedupeSkipsDuplicateItems() async {
        let config = TrendDetector.Configuration(
            shortWindow: 300,
            baselineWindow: 900,
            bucketSize: 60,
            enableBigrams: false,
            enableTrigrams: false,
            enableTitleCasePhrases: false,
            sampleHeadlineLimit: 1,
            minShortCount: 1,
            minUniqueSources: 1,
            enableDedupe: true
        )
        let detector = TrendDetector(configuration: config)
        let now = Date()

        let item = NewsItem(
            feedID: "feed-a",
            source: "SourceA",
            title: "Unique topic",
            body: nil,
            url: URL(string: "https://example.com/dup")!,
            publishedAt: now,
            ingestedAt: now
        )

        await detector.ingest(item)
        await detector.ingest(item)
        let topics = await detector.trending(now: now)

        let uniqueCount = topics.first { $0.term == "unique" }?.shortCount ?? 0
        XCTAssertEqual(uniqueCount, 1)
    }

    func testPerSourceCapSkipsExtraItems() async {
        let config = TrendDetector.Configuration(
            shortWindow: 300,
            baselineWindow: 900,
            bucketSize: 60,
            enableBigrams: false,
            enableTrigrams: false,
            enableTitleCasePhrases: false,
            sampleHeadlineLimit: 1,
            minShortCount: 1,
            minUniqueSources: 1,
            maxItemsPerSourcePerBucket: 1
        )
        let detector = TrendDetector(configuration: config)
        let now = Date()

        let itemA = NewsItem(
            feedID: "feed-a",
            source: "SourceA",
            title: "Alpha signal",
            body: nil,
            url: URL(string: "https://example.com/a")!,
            publishedAt: now,
            ingestedAt: now
        )
        let itemB = NewsItem(
            feedID: "feed-a",
            source: "SourceA",
            title: "Beta signal",
            body: nil,
            url: URL(string: "https://example.com/b")!,
            publishedAt: now,
            ingestedAt: now
        )

        await detector.ingest([itemA, itemB])
        let topics = await detector.trending(now: now)

        XCTAssertTrue(topics.contains { $0.term == "alpha" })
        XCTAssertFalse(topics.contains { $0.term == "beta" })
    }
}
