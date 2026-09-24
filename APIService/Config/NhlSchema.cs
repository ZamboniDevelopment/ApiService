namespace APIService.Config;

public enum NhlReportMode { Vs, So, Otp }

public enum NhlUserHistoryStyle
{
    /// NHL10: single table, full row, explicitly ORDER BY created_at DESC, merges 5 opponent
    /// fields (opponent, opponent_team, opponent_score, opponent_hits, opponent_shots).
    FullOrderedWithHitsAndShots,

    /// NHL11 / NHL12: UNION ALL across sources with a REDUCED column projection
    /// (game_id, user_id, gamertag, team_name, score, created_at), no explicit ordering,
    /// flat array, merges 3 opponent fields (opponent, opponent_team, opponent_score).
    ReducedFlatUnordered,

    /// Shared: full row per source, no explicit ordering, grouped {VS, SO} (not flat),
    /// merges the same 3 opponent fields as above.
    GroupedFullUnordered
}

public sealed class NhlReportSource
{
    public NhlReportMode Mode { get; init; }
    public string JsonKey { get; init; } = "";   // "VS" / "SO" / "OTP" in resp
    public string Table { get; init; } = "";
}
public sealed class NhlColumns
{
    public string GamerTag { get; init; } = "gamertag";
    public string Score { get; init; } = "score";
    public string TeamName { get; init; } = "team_name";
    public string Shots { get; init; } = "shots";
    public string HomeIndicator { get; init; } = "home";
}

public sealed class NhlCacheOptions
{
    public TimeSpan? Players { get; init; }
    public TimeSpan? Player { get; init; }
    public TimeSpan? Games { get; init; }
    public TimeSpan? Leaderboard { get; init; }
    public TimeSpan? GlobalStats { get; init; }
}

public sealed class NhlSchema
{
    public string GamesTable { get; init; } = "games";
    public IReadOnlyList<NhlReportSource> Sources { get; init; } = Array.Empty<NhlReportSource>();
    public NhlColumns Columns { get; init; } = new();
    public NhlCacheOptions Cache { get; init; } = new();
    public string CacheKeyPrefix { get; init; } = "nhl";
    public bool RawGamesOrderedByCreatedAt { get; init; } = false;

    public NhlUserHistoryStyle UserHistoryStyle { get; init; } = NhlUserHistoryStyle.ReducedFlatUnordered;
    
    public bool LowercasePlayerCacheKey { get; init; } = false;
    
    public bool GameReportsIncludesGameIdField { get; init; } = false;
}
