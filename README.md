# DoomTrends

Lightweight, heuristic trend detection for Doomberg. It ingests `NewsItem` values and
emits a ranked list of trending terms based on short-window burst, acceleration, and
cross-source presence.

## Features

- Rolling, time-bucketed counts for short and baseline windows
- Token + phrase extraction (bigrams, trigrams, Title-Case capture)
- URL fragments are stripped before tokenization
- Short proper nouns and all-caps acronyms are preserved (e.g. "Wu", "US")
- Explainable output (counts, acceleration, sample headlines)
- Alias mapping support (loaded by your orchestration layer)

## Usage

```swift
import DoomTrends
import DoomModels

let detector = TrendDetector()

await detector.ingest(NewsItem(
    feedID: "bbc-world",
    source: "BBC",
    title: "Oil prices surge on supply fears",
    body: nil,
    url: URL(string: "https://example.com/oil")!,
    publishedAt: Date(),
    ingestedAt: Date()
))

let topics = await detector.trending()
```

## Output Fields

Each `TrendingTopic` contains:

- `term`: canonicalized term or phrase
- `score`: composite score for ranking (burst + acceleration + sources)
- `shortCount`: count within the short window
- `baselineCount`: count within the baseline window
- `uniqueSources`: distinct sources in the short window
- `acceleration`: `shortCount - prevShortCount`
- `sampleHeadlines`: representative headlines for explainability
- `lastSeenAt`: most recent timestamp for the term

## Alias Mapping

The orchestration layer can load a JSON dictionary and pass it in via the public API.
Keys and values are lowercased internally.

```swift
await detector.updateAliasMap([
    "fed": "federal reserve",
    "boe": "bank of england"
])
```

You can also merge additional mappings:

```swift
await detector.addAliasMappings([
    "ftc": "federal trade commission"
])
```

## Stopword Updates

Stopwords can be updated at runtime. Inputs are lowercased internally.

```swift
await detector.addStopwords(["whatsup", "breaking"])
await detector.removeStopwords(["breaking"])
```

## Configuration

```swift
let config = TrendDetector.Configuration(
    shortWindow: 15 * 60,
    baselineWindow: 6 * 60 * 60,
    bucketSize: 60,
    enableBigrams: true,
    enableTrigrams: false,
    enableTitleCasePhrases: true,
    minShortCount: 2,
    minUniqueSources: 1
)
let detector = TrendDetector(configuration: config)
```

## Build

```bash
swift build
```

## Test

```bash
swift test
```
