import Foundation
import DoomModels

#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#endif

public struct TrendingTopic: Sendable, Codable, Hashable {
    public let term: String
    public let score: Double
    public let shortCount: Int
    public let baselineCount: Int
    public let uniqueSources: Int
    public let acceleration: Int
    public let sampleHeadlines: [String]
    public let lastSeenAt: Date

    public init(
        term: String,
        score: Double,
        shortCount: Int,
        baselineCount: Int,
        uniqueSources: Int,
        acceleration: Int,
        sampleHeadlines: [String],
        lastSeenAt: Date
    ) {
        self.term = term
        self.score = score
        self.shortCount = shortCount
        self.baselineCount = baselineCount
        self.uniqueSources = uniqueSources
        self.acceleration = acceleration
        self.sampleHeadlines = sampleHeadlines
        self.lastSeenAt = lastSeenAt
    }
}

public actor TrendDetector {
    public struct Configuration: Sendable, Codable, Hashable {
        public var shortWindow: TimeInterval
        public var baselineWindow: TimeInterval
        public var bucketSize: TimeInterval
        public var maxTermsPerItem: Int
        public var minTokenLength: Int
        public var enableBigrams: Bool
        public var enableTrigrams: Bool
        public var enableTitleCasePhrases: Bool
        public var allowNumericTokens: Bool
        public var summaryMaxLength: Int

        public var stopwords: Set<String>
        public var bannedTerms: Set<String>
        public var aliasMap: [String: String]
        public var filterStopwordsInPhrases: Bool
        public var phraseStopwords: Set<String>
        public var enableDedupe: Bool
        public var maxItemsPerSourcePerBucket: Int?

        public var weights: Weights
        public struct Weights: Sendable, Codable, Hashable {
            public var accelWeight: Double
            public var sourceWeight: Double
            public var countLogWeight: Double
            public var titleCaseWeight: Double
            public var allCapsWeight: Double
            public var titleTokenWeight: Double
            public var bodyTokenWeight: Double

            public init(
                accelWeight: Double = 0.25,
                sourceWeight: Double = 0.30,
                countLogWeight: Double = 1.0,
                titleCaseWeight: Double = 1.25,
                allCapsWeight: Double = 1.5,
                titleTokenWeight: Double = 1.0,
                bodyTokenWeight: Double = 0.6
            ) {
                self.accelWeight = accelWeight
                self.sourceWeight = sourceWeight
                self.countLogWeight = countLogWeight
                self.titleCaseWeight = titleCaseWeight
                self.allCapsWeight = allCapsWeight
                self.titleTokenWeight = titleTokenWeight
                self.bodyTokenWeight = bodyTokenWeight
            }
        }

        public var topicLimit: Int
        public var sampleHeadlineLimit: Int
        public var minShortCount: Int
        public var minUniqueSources: Int
        public var lowercaseMinShortCount: Int
        public var lowercaseMinUniqueSources: Int

        public init(
            shortWindow: TimeInterval = 15 * 60,
            baselineWindow: TimeInterval = 6 * 60 * 60,
            bucketSize: TimeInterval = 60,
            maxTermsPerItem: Int = 40,
            minTokenLength: Int = 3,
            enableBigrams: Bool = true,
            enableTrigrams: Bool = false,
            enableTitleCasePhrases: Bool = true,
            allowNumericTokens: Bool = false,
            summaryMaxLength: Int = 500,
            stopwords: Set<String> = Configuration.defaultStopwords,
            bannedTerms: Set<String> = [],
            aliasMap: [String: String] = [:],
            filterStopwordsInPhrases: Bool = false,
            phraseStopwords: Set<String> = Configuration.defaultPhraseStopwords,
            weights: Weights = Weights(),
            topicLimit: Int = 30,
            sampleHeadlineLimit: Int = 5,
            minShortCount: Int = 2,
            minUniqueSources: Int = 2,
            lowercaseMinShortCount: Int? = nil,
            lowercaseMinUniqueSources: Int? = nil,
            enableDedupe: Bool = true,
            maxItemsPerSourcePerBucket: Int? = 5
        ) {
            self.shortWindow = shortWindow
            self.baselineWindow = baselineWindow
            self.bucketSize = bucketSize
            self.maxTermsPerItem = maxTermsPerItem
            self.minTokenLength = minTokenLength
            self.enableBigrams = enableBigrams
            self.enableTrigrams = enableTrigrams
            self.enableTitleCasePhrases = enableTitleCasePhrases
            self.allowNumericTokens = allowNumericTokens
            self.summaryMaxLength = summaryMaxLength
            self.stopwords = stopwords
            self.bannedTerms = bannedTerms
            self.aliasMap = aliasMap
            self.filterStopwordsInPhrases = filterStopwordsInPhrases
            self.phraseStopwords = phraseStopwords
            self.weights = weights
            self.topicLimit = topicLimit
            self.sampleHeadlineLimit = sampleHeadlineLimit
            self.minShortCount = minShortCount
            self.minUniqueSources = minUniqueSources
            self.lowercaseMinShortCount = lowercaseMinShortCount ?? max(minShortCount, 3)
            self.lowercaseMinUniqueSources = lowercaseMinUniqueSources ?? minUniqueSources
            self.enableDedupe = enableDedupe
            self.maxItemsPerSourcePerBucket = maxItemsPerSourcePerBucket
        }

        public static let defaultStopwords: Set<String> = [
            "a", "about", "according", "across", "access", "action", "actions",
            "administration", "advances", "after", "against", "age", "agency", "agent",
            "all", "amid", "america", "an", "and", "agents", "another", "are", "areas",
            "as", "at", "authorities", "back", "bar", "be", "been", "before",
            "being", "blank", "border", "breaking", "breaks", "business", "but",
            "by", "called", "calling", "came", "car", "changed", "child", "children",
            "citizens", "city", "claims", "closed", "coast", "cold", "concerns",
            "conditions", "continues", "customs", "damage", "death", "deadly",
            "defends", "department", "despite", "disruption", "downplay",
            "enforcement", "eve", "exclusive", "experience", "explore", "extension",
            "extending", "fatal", "fatally", "father", "federal", "fire", "first",
            "flight", "focus", "following", "for", "force", "forces", "former",
            "found", "forward", "four", "from", "funding", "game", "general", "get",
            "gets", "going", "good", "got", "government", "growing", "guard", "has",
            "have", "he", "health", "held", "help", "her", "heres", "high", "his",
            "hit", "holds", "homeland", "homes", "host", "house", "how", "if", "in",
            "incident", "incidents", "including", "interview", "into",
            "investigation", "involved", "is", "issued", "issues", "it", "its",
            "jan", "joins", "judge", "key", "killing", "know", "large", "late",
            "latest", "law", "leader", "leaders", "lead", "led", "life", "like",
            "linked", "live", "major", "man", "mark", "massive", "me", "measure",
            "measures", "meeting", "media", "met", "million", "more", "morning",
            "move", "my", "new", "news", "next", "night", "no", "north", "not",
            "of", "off", "office", "officer", "officers", "official", "officials",
            "old", "on", "one", "opinion", "or", "our", "out", "over", "owner",
            "party", "pass", "patients", "plan", "point", "power", "powers",
            "previously", "property", "prosecutor", "rate", "really", "remains",
            "report", "reporter", "reports", "response", "return", "reveals",
            "review", "risk", "say", "says", "said", "scrutiny", "second", "security",
            "seek", "seems", "sell", "services", "set", "seizure", "she", "shes",
            "shut", "since", "side", "slams", "so", "sold", "sources", "spoke",
            "special", "star", "state", "stay", "still", "streets", "suffers",
            "support", "suspected", "taking", "takes", "talk", "talks", "target",
            "team", "tensions", "than", "that", "the", "their", "them", "then",
            "there", "these", "they", "this", "those", "three", "time", "to",
            "took", "toward", "trying", "trumps", "two", "turning", "under", "up",
            "update", "updates", "urged", "us", "use", "vehicle", "vehicles", "vote",
            "warnings", "warns", "was", "watch", "way", "we", "were", "what", "when",
            "where", "which", "white", "who", "why", "winds", "with", "woman", "want",
            "you", "your",
            "near", "people",
            "can", "cant", "could", "couldnt", "may", "might", "must", "should",
            "shouldnt", "will", "wont", "would", "wouldnt", "dont", "doesnt",
            "didnt", "isnt", "arent", "wasnt", "werent", "hasnt", "havent", "hadnt",
            "day", "days", "week", "weeks", "month", "months", "year", "years",
            "today", "tonight", "yesterday", "tomorrow",
            "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday",
            "january", "february", "march", "april", "may", "june", "july", "august",
            "september", "october", "november", "december",
            "file", "files", "video", "videos"
        ]

        public static let defaultPhraseStopwords: Set<String> = [
            "guard", "guards", "guarded", "guarding",
            "says", "said", "say", "saying",
            "warn", "warns", "warned", "warning",
            "calls", "called", "calling",
            "backs", "backed", "backing",
            "reports", "reported", "reporting",
            "sees", "saw", "seeing"
        ]
    }

    private struct Bucket {
        var termCounts: [String: Int] = [:]
        var termTitleCounts: [String: Int] = [:]
        var termSources: [String: Set<String>] = [:]
        var sourceItemCounts: [String: Int] = [:]
    }

    private enum CaseStyle: Int, Comparable {
        case normal = 0
        case titleCase = 1
        case allCaps = 2

        static func < (lhs: CaseStyle, rhs: CaseStyle) -> Bool {
            lhs.rawValue < rhs.rawValue
        }
    }

    private struct TermCandidate {
        let term: String
        let caseStyle: CaseStyle
        let isTitle: Bool
    }

    private struct Token {
        let value: String
        let caseStyle: CaseStyle
    }

    private struct HeadlineSample: Hashable {
        let headline: String
        let sourceID: String
        let publishedAt: Date
    }

    private var configuration: Configuration
    private var buckets: [Int: Bucket] = [:]
    private var termSamples: [String: [HeadlineSample]] = [:]
    private var lastSeenAt: [String: Date] = [:]
    private var termCaseStyles: [String: CaseStyle] = [:]
    private var recentURLSeen: [String: Date] = [:]
    private var recentTitleSeen: [String: Date] = [:]

    public init(configuration: Configuration = Configuration()) {
        var normalizedConfig = configuration
        if !configuration.aliasMap.isEmpty {
            var normalized: [String: String] = [:]
            normalized.reserveCapacity(configuration.aliasMap.count)
            for (key, value) in configuration.aliasMap {
                normalized[key.lowercased()] = value.lowercased()
            }
            normalizedConfig.aliasMap = normalized
        }
        self.configuration = normalizedConfig
    }

    public func ingest(_ item: NewsItem) {
        ingest([item])
    }

    public func ingest(_ items: [NewsItem]) {
        guard !items.isEmpty else { return }
        for item in items {
            ingestSingle(item)
        }
        let cutoff = Date().addingTimeInterval(-configuration.baselineWindow)
        expireBuckets(olderThan: cutoff)
        expireDedupe(olderThan: cutoff)
    }

    public func trending(now: Date = .init()) -> [TrendingTopic] {
        let cutoff = now.addingTimeInterval(-configuration.baselineWindow)
        expireBuckets(olderThan: cutoff)
        expireDedupe(olderThan: cutoff)
        pruneSamples(olderThan: cutoff)

        let shortStart = now.addingTimeInterval(-configuration.shortWindow)
        let prevShortStart = now.addingTimeInterval(-configuration.shortWindow * 2)
        let baselineStart = now.addingTimeInterval(-configuration.baselineWindow)

        var shortCounts: [String: Int] = [:]
        var prevShortCounts: [String: Int] = [:]
        var baselineCounts: [String: Int] = [:]
        var shortTitleCounts: [String: Int] = [:]
        var prevShortTitleCounts: [String: Int] = [:]
        var baselineTitleCounts: [String: Int] = [:]
        var shortSources: [String: Set<String>] = [:]

        for (bucketKey, bucket) in buckets {
            let bucketStart = date(forBucketKey: bucketKey)
            if bucketStart < baselineStart {
                continue
            }
            let inShort = bucketStart >= shortStart
            let inPrevShort = bucketStart >= prevShortStart && bucketStart < shortStart

            for (term, count) in bucket.termCounts {
                let titleCount = bucket.termTitleCounts[term, default: 0]
                baselineCounts[term, default: 0] += count
                baselineTitleCounts[term, default: 0] += titleCount
                if inShort {
                    shortCounts[term, default: 0] += count
                    shortTitleCounts[term, default: 0] += titleCount
                } else if inPrevShort {
                    prevShortCounts[term, default: 0] += count
                    prevShortTitleCounts[term, default: 0] += titleCount
                }
            }

            if inShort {
                for (term, sources) in bucket.termSources {
                    if shortSources[term] == nil {
                        shortSources[term] = sources
                    } else {
                        shortSources[term]?.formUnion(sources)
                    }
                }
            }
        }

        var topics: [TrendingTopic] = []
        topics.reserveCapacity(shortCounts.count)

        for (term, shortCount) in shortCounts {
            let uniqueSources = shortSources[term]?.count ?? 0
            let baselineCount = baselineCounts[term, default: 0]
            let prevShortCount = prevShortCounts[term, default: 0]

            let shortTitleCount = shortTitleCounts[term, default: 0]
            let prevShortTitleCount = prevShortTitleCounts[term, default: 0]
            let baselineTitleCount = baselineTitleCounts[term, default: 0]

            let weightedShort = weightedCount(total: shortCount, title: shortTitleCount)
            let weightedPrevShort = weightedCount(total: prevShortCount, title: prevShortTitleCount)
            let weightedBaseline = weightedCount(total: baselineCount, title: baselineTitleCount)

            let caseStyle = termCaseStyles[term] ?? .normal
            let minShort = caseStyle == .normal ? configuration.lowercaseMinShortCount : configuration.minShortCount
            let minSources = caseStyle == .normal ? configuration.lowercaseMinUniqueSources : configuration.minUniqueSources

            if weightedShort < Double(minShort) {
                continue
            }
            if uniqueSources < minSources {
                continue
            }

            let acceleration = shortCount - prevShortCount
            let weightedAcceleration = weightedShort - weightedPrevShort
            let burst = weightedShort / max(1.0, weightedBaseline)
            let countFactor = configuration.weights.countLogWeight * log(1 + weightedShort)
            let accelFactor = 1 + configuration.weights.accelWeight * max(0, weightedAcceleration)
            let sourceFactor = 1 + configuration.weights.sourceWeight * max(0, Double(uniqueSources - 1))
            let caseFactor = caseWeight(for: term)
            let score = countFactor * burst * accelFactor * sourceFactor * caseFactor
            let samples = sampleHeadlines(for: term, limit: configuration.sampleHeadlineLimit)
            let lastSeen = lastSeenAt[term] ?? now

            topics.append(
                TrendingTopic(
                    term: term,
                    score: score,
                    shortCount: shortCount,
                    baselineCount: baselineCount,
                    uniqueSources: uniqueSources,
                    acceleration: acceleration,
                    sampleHeadlines: samples,
                    lastSeenAt: lastSeen
                )
            )
        }

        topics.sort {
            if $0.score != $1.score {
                return $0.score > $1.score
            }
            if $0.shortCount != $1.shortCount {
                return $0.shortCount > $1.shortCount
            }
            return $0.lastSeenAt > $1.lastSeenAt
        }

        if topics.count > configuration.topicLimit {
            return Array(topics.prefix(configuration.topicLimit))
        }
        return topics
    }

    public func reset() {
        buckets.removeAll()
        termSamples.removeAll()
        lastSeenAt.removeAll()
        termCaseStyles.removeAll()
        recentURLSeen.removeAll()
        recentTitleSeen.removeAll()
    }

    public func updateAliasMap(_ aliasMap: [String: String]) {
        var normalized: [String: String] = [:]
        normalized.reserveCapacity(aliasMap.count)
        for (key, value) in aliasMap {
            normalized[key.lowercased()] = value.lowercased()
        }
        configuration.aliasMap = normalized
    }

    public func addAliasMappings(_ mappings: [String: String]) {
        for (key, value) in mappings {
            configuration.aliasMap[key.lowercased()] = value.lowercased()
        }
    }

    public func addStopwords(_ stopwords: [String]) {
        for word in stopwords {
            configuration.stopwords.insert(word.lowercased())
        }
    }

    public func removeStopwords(_ stopwords: [String]) {
        for word in stopwords {
            configuration.stopwords.remove(word.lowercased())
        }
    }

    public func addPhraseStopwords(_ stopwords: [String]) {
        for word in stopwords {
            configuration.phraseStopwords.insert(word.lowercased())
        }
    }

    public func removePhraseStopwords(_ stopwords: [String]) {
        for word in stopwords {
            configuration.phraseStopwords.remove(word.lowercased())
        }
    }

    private func ingestSingle(_ item: NewsItem) {
        let timestamp = item.publishedAt
        let cutoff = timestamp.addingTimeInterval(-configuration.baselineWindow)
        if configuration.enableDedupe {
            if isDuplicate(item, cutoff: cutoff) {
                return
            }
            recordDedupe(item, timestamp: timestamp)
        }

        let bucketKey = bucketKey(for: timestamp)
        var bucket = buckets[bucketKey, default: Bucket()]

        let sourceID = item.source
        if let cap = configuration.maxItemsPerSourcePerBucket, cap > 0 {
            let count = bucket.sourceItemCounts[sourceID, default: 0]
            if count >= cap {
                buckets[bucketKey] = bucket
                return
            }
            bucket.sourceItemCounts[sourceID] = count + 1
        }

        let terms = extractTerms(from: item)
        guard !terms.isEmpty else {
            buckets[bucketKey] = bucket
            return
        }

        for candidate in terms {
            let term = candidate.term
            bucket.termCounts[term, default: 0] += 1
            if candidate.isTitle {
                bucket.termTitleCounts[term, default: 0] += 1
            }
            if bucket.termSources[term] == nil {
                bucket.termSources[term] = [sourceID]
            } else {
                bucket.termSources[term]?.insert(sourceID)
            }
            addSample(term: term, headline: item.title, sourceID: sourceID, publishedAt: timestamp)
            recordCaseStyle(term: term, caseStyle: candidate.caseStyle)
            if let existing = lastSeenAt[term] {
                if timestamp > existing {
                    lastSeenAt[term] = timestamp
                }
            } else {
                lastSeenAt[term] = timestamp
            }
        }

        buckets[bucketKey] = bucket
    }

    private func extractTerms(from item: NewsItem) -> [TermCandidate] {
        let title = stripURLSubstrings(from: item.title)
        let summary = item.body?.prefix(configuration.summaryMaxLength) ?? ""
        let body = stripURLSubstrings(from: String(summary))

        let titleTokens = normalizedTokens(from: title)
        let bodyTokens = normalizedTokens(from: body)

        var terms: [TermCandidate] = []
        var seen: [String: Int] = [:]

        func addTerm(_ term: String, caseStyle: CaseStyle, isTitle: Bool) {
            guard terms.count < configuration.maxTermsPerItem else { return }
            if let index = seen[term] {
                let current = terms[index]
                let mergedCase = max(current.caseStyle, caseStyle)
                let mergedTitle = current.isTitle || isTitle
                if mergedCase != current.caseStyle || mergedTitle != current.isTitle {
                    terms[index] = TermCandidate(term: term, caseStyle: mergedCase, isTitle: mergedTitle)
                }
                return
            }
            seen[term] = terms.count
            terms.append(TermCandidate(term: term, caseStyle: caseStyle, isTitle: isTitle))
        }

        func addTokens(_ tokens: [Token], isTitle: Bool) {
            for token in tokens {
                addTerm(canonicalize(term: token.value), caseStyle: token.caseStyle, isTitle: isTitle)
            }

            if configuration.enableBigrams && tokens.count >= 2 {
                for index in 0..<(tokens.count - 1) {
                    let phrase = tokens[index].value + " " + tokens[index + 1].value
                    if shouldFilterPhrase(phrase, includeStopwords: configuration.filterStopwordsInPhrases) {
                        continue
                    }
                    let phraseCase = caseStyle(for: [tokens[index], tokens[index + 1]])
                    addTerm(canonicalize(term: phrase), caseStyle: phraseCase, isTitle: isTitle)
                }
            }

            if configuration.enableTrigrams && tokens.count >= 3 {
                for index in 0..<(tokens.count - 2) {
                    let phrase = tokens[index].value + " " + tokens[index + 1].value + " " + tokens[index + 2].value
                    if shouldFilterPhrase(phrase, includeStopwords: configuration.filterStopwordsInPhrases) {
                        continue
                    }
                    let phraseCase = caseStyle(for: [tokens[index], tokens[index + 1], tokens[index + 2]])
                    addTerm(canonicalize(term: phrase), caseStyle: phraseCase, isTitle: isTitle)
                }
            }
        }

        addTokens(titleTokens, isTitle: true)
        addTokens(bodyTokens, isTitle: false)

        if configuration.enableTitleCasePhrases {
            let phrases = titleCasePhrases(from: item.title)
            for phrase in phrases {
                addTerm(canonicalize(term: phrase), caseStyle: .titleCase, isTitle: true)
            }
        }

        return terms
    }

    private func normalizedTokens(from text: String) -> [Token] {
        var buffer: [String] = []
        buffer.reserveCapacity(64)
        var current = ""
        current.reserveCapacity(16)

        for scalar in text.unicodeScalars {
            if CharacterSet.alphanumerics.contains(scalar) {
                current.unicodeScalars.append(scalar)
            } else if isApostropheScalar(scalar) {
                if !current.isEmpty {
                    current.unicodeScalars.append(scalar)
                }
            } else {
                if !current.isEmpty {
                    buffer.append(current)
                    current.removeAll(keepingCapacity: true)
                }
            }
        }
        if !current.isEmpty {
            buffer.append(current)
        }

        var tokens: [Token] = []
        tokens.reserveCapacity(buffer.count)
        for raw in buffer {
            let stripped = stripApostrophes(from: raw)
            if stripped.isEmpty { continue }

            let isAllCaps = isAllCapsToken(stripped)
            let isTitleCase = !isAllCaps && isTitleCaseToken(stripped)
            let isShort = stripped.count < configuration.minTokenLength
            let allowShort = isShort && (isAllCaps || isShortProperNoun(stripped))

            if isShort && !allowShort { continue }

            let normalized = stripped.lowercased()
            if configuration.bannedTerms.contains(normalized) { continue }
            if configuration.stopwords.contains(normalized) && !isAllCaps { continue }
            if !configuration.allowNumericTokens && normalized.unicodeScalars.allSatisfy({ CharacterSet.decimalDigits.contains($0) }) {
                continue
            }

            let tokenValue = isAllCaps ? stripped.uppercased() : normalized
            let caseStyle: CaseStyle = isAllCaps ? .allCaps : (isTitleCase ? .titleCase : .normal)
            tokens.append(Token(value: tokenValue, caseStyle: caseStyle))
        }
        return tokens
    }

    private func titleCasePhrases(from title: String) -> [String] {
        let rawTokens = title.split { $0.isWhitespace }
        let tokens = rawTokens.map { trimNonAlnum(String($0)) }.filter { !$0.isEmpty }
        guard tokens.count >= 2 else { return [] }

        var phrases: [String] = []
        var current: [String] = []

        func flushCurrent() {
            guard current.count >= 2 else {
                current.removeAll(keepingCapacity: true)
                return
            }
            let maxLen = min(5, current.count)
            for start in 0..<(current.count - 1) {
                for length in 2...maxLen {
                    if start + length > current.count { break }
                    let phrase = current[start..<(start + length)].joined(separator: " ").lowercased()
                    if shouldFilterPhrase(phrase, includeStopwords: configuration.filterStopwordsInPhrases) { continue }
                    phrases.append(phrase)
                }
            }
            current.removeAll(keepingCapacity: true)
        }

        for token in tokens {
            if isTitleCaseToken(token) {
                current.append(token)
            } else {
                flushCurrent()
            }
        }
        flushCurrent()

        return phrases
    }

    private func caseStyle(for tokens: [Token]) -> CaseStyle {
        guard !tokens.isEmpty else { return .normal }
        if tokens.allSatisfy({ $0.caseStyle == .allCaps }) {
            return .allCaps
        }
        if tokens.allSatisfy({ $0.caseStyle == .titleCase || $0.caseStyle == .allCaps }) {
            return .titleCase
        }
        return .normal
    }

    private func isTitleCaseToken(_ token: String) -> Bool {
        guard let firstScalar = token.unicodeScalars.first else { return false }
        if !CharacterSet.uppercaseLetters.contains(firstScalar) {
            return false
        }
        let hasLowercase = token.unicodeScalars.contains { CharacterSet.lowercaseLetters.contains($0) }
        return hasLowercase || token == token.uppercased()
    }

    private func trimNonAlnum(_ value: String) -> String {
        let trimmed = value.trimmingCharacters(in: CharacterSet.alphanumerics.inverted)
        return trimmed
    }

    private func shouldFilterPhrase(_ phrase: String, includeStopwords: Bool) -> Bool {
        let parts = phrase.split(separator: " ")
        for part in parts {
            let token = part.lowercased()
            if includeStopwords && configuration.stopwords.contains(token) { return true }
            if configuration.bannedTerms.contains(token) { return true }
            if configuration.phraseStopwords.contains(token) { return true }
        }
        return false
    }

    private func canonicalize(term: String) -> String {
        let lowered = term.lowercased()
        if let mapped = configuration.aliasMap[lowered] {
            return mapped
        }
        if isAllCapsToken(term) {
            return term.uppercased()
        }
        return lowered
    }

    private func recordCaseStyle(term: String, caseStyle: CaseStyle) {
        guard caseStyle != .normal else { return }
        if let existing = termCaseStyles[term] {
            if caseStyle > existing {
                termCaseStyles[term] = caseStyle
            }
        } else {
            termCaseStyles[term] = caseStyle
        }
    }

    private func caseWeight(for term: String) -> Double {
        let style = termCaseStyles[term] ?? .normal
        switch style {
        case .allCaps:
            return configuration.weights.allCapsWeight
        case .titleCase:
            return configuration.weights.titleCaseWeight
        case .normal:
            return 1.0
        }
    }

    private func weightedCount(total: Int, title: Int) -> Double {
        let titleWeight = configuration.weights.titleTokenWeight
        let bodyWeight = configuration.weights.bodyTokenWeight
        let titleCount = max(0, title)
        let bodyCount = max(0, total - titleCount)
        return Double(titleCount) * titleWeight + Double(bodyCount) * bodyWeight
    }

    private func isDuplicate(_ item: NewsItem, cutoff: Date) -> Bool {
        let urlKey = item.url.absoluteString.lowercased()
        if let seen = recentURLSeen[urlKey], seen >= cutoff {
            return true
        }

        let titleKey = normalizeTitleForDedupe(item.title)
        if let seen = recentTitleSeen[titleKey], seen >= cutoff {
            return true
        }

        return false
    }

    private func recordDedupe(_ item: NewsItem, timestamp: Date) {
        let urlKey = item.url.absoluteString.lowercased()
        recentURLSeen[urlKey] = timestamp
        let titleKey = normalizeTitleForDedupe(item.title)
        recentTitleSeen[titleKey] = timestamp
    }

    private func expireDedupe(olderThan cutoff: Date) {
        recentURLSeen.keys.filter { (recentURLSeen[$0] ?? cutoff) < cutoff }.forEach {
            recentURLSeen.removeValue(forKey: $0)
        }
        recentTitleSeen.keys.filter { (recentTitleSeen[$0] ?? cutoff) < cutoff }.forEach {
            recentTitleSeen.removeValue(forKey: $0)
        }
    }

    private func normalizeTitleForDedupe(_ title: String) -> String {
        title.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    }

    private func stripApostrophes(from value: String) -> String {
        var result = String.UnicodeScalarView()
        result.reserveCapacity(value.unicodeScalars.count)
        for scalar in value.unicodeScalars where !isApostropheScalar(scalar) {
            result.append(scalar)
        }
        return String(result)
    }

    private func isAllCapsToken(_ token: String) -> Bool {
        var letterCount = 0
        for scalar in token.unicodeScalars {
            if CharacterSet.letters.contains(scalar) {
                letterCount += 1
                if !CharacterSet.uppercaseLetters.contains(scalar) {
                    return false
                }
            }
        }
        return letterCount >= 2
    }

    private func isShortProperNoun(_ token: String) -> Bool {
        guard token.count >= 2, token.count < configuration.minTokenLength else { return false }
        guard let first = token.unicodeScalars.first, CharacterSet.uppercaseLetters.contains(first) else {
            return false
        }
        var hasLowercase = false
        var hasUppercaseAfterFirst = false
        for scalar in token.unicodeScalars.dropFirst() {
            if CharacterSet.uppercaseLetters.contains(scalar) {
                hasUppercaseAfterFirst = true
            }
            if CharacterSet.lowercaseLetters.contains(scalar) {
                hasLowercase = true
            }
        }
        return hasLowercase && !hasUppercaseAfterFirst
    }

    private func stripURLSubstrings(from text: String) -> String {
        let segments = text.split(whereSeparator: { $0.isWhitespace })
        guard segments.count > 0 else { return text }
        var cleaned: [String] = []
        cleaned.reserveCapacity(segments.count)

        for segment in segments {
            let token = String(segment)
            if isURLLikeSegment(token) {
                continue
            }
            cleaned.append(token)
        }

        return cleaned.joined(separator: " ")
    }

    private func isURLLikeSegment(_ segment: String) -> Bool {
        let lower = segment.lowercased()
        if lower.hasPrefix("http://") || lower.hasPrefix("https://") || lower.hasPrefix("www.") {
            return true
        }

        let trimmed = segment.trimmingCharacters(in: CharacterSet.punctuationCharacters.union(.symbols))
        if trimmed.contains("@") || trimmed.contains("/") {
            return true
        }

        let parts = trimmed.split(separator: ".")
        guard parts.count >= 2 else { return false }
        guard let tld = parts.last?.lowercased() else { return false }
        if Self.commonTLDs.contains(tld) {
            return true
        }

        return false
    }

    private static let commonTLDs: Set<String> = [
        "com", "net", "org", "io", "co", "gov", "edu", "uk", "us", "de", "jp", "fr",
        "it", "ru", "cn", "info", "biz", "me", "tv", "ai"
    ]

    private func isApostropheScalar(_ scalar: Unicode.Scalar) -> Bool {
        switch scalar.value {
        case 0x27, 0x2018, 0x2019:
            return true
        default:
            return false
        }
    }

    private func addSample(term: String, headline: String, sourceID: String, publishedAt: Date) {
        guard configuration.sampleHeadlineLimit > 0 else { return }
        let sample = HeadlineSample(headline: headline, sourceID: sourceID, publishedAt: publishedAt)
        var samples = termSamples[term, default: []]

        if let index = samples.firstIndex(where: { $0.headline == headline }) {
            if publishedAt > samples[index].publishedAt {
                samples[index] = sample
            }
        } else {
            samples.append(sample)
        }

        samples.sort { $0.publishedAt > $1.publishedAt }
        let cap = max(1, configuration.sampleHeadlineLimit * 3)
        if samples.count > cap {
            samples = Array(samples.prefix(cap))
        }
        termSamples[term] = samples
    }

    private func sampleHeadlines(for term: String, limit: Int) -> [String] {
        guard limit > 0 else { return [] }
        guard let samples = termSamples[term], !samples.isEmpty else { return [] }

        var selected: [String] = []
        var usedSources: Set<String> = []
        for sample in samples {
            if selected.count >= limit { break }
            if usedSources.contains(sample.sourceID) { continue }
            selected.append(sample.headline)
            usedSources.insert(sample.sourceID)
        }
        if selected.count < limit {
            for sample in samples {
                if selected.count >= limit { break }
                if selected.contains(sample.headline) { continue }
                selected.append(sample.headline)
            }
        }
        return selected
    }

    private func bucketKey(for date: Date) -> Int {
        let interval = date.timeIntervalSince1970
        let size = max(1.0, configuration.bucketSize)
        return Int(floor(interval / size))
    }

    private func date(forBucketKey key: Int) -> Date {
        let size = max(1.0, configuration.bucketSize)
        return Date(timeIntervalSince1970: TimeInterval(key) * size)
    }

    private func expireBuckets(olderThan cutoff: Date) {
        let minKey = bucketKey(for: cutoff)
        buckets.keys.filter { $0 < minKey }.forEach { buckets.removeValue(forKey: $0) }
    }

    private func pruneSamples(olderThan cutoff: Date) {
        for (term, samples) in termSamples {
            let filtered = samples.filter { $0.publishedAt >= cutoff }
            if filtered.isEmpty {
                termSamples.removeValue(forKey: term)
            } else {
                termSamples[term] = filtered
            }
        }
        for (term, lastSeen) in lastSeenAt where lastSeen < cutoff {
            lastSeenAt.removeValue(forKey: term)
            termCaseStyles.removeValue(forKey: term)
        }
    }
}
