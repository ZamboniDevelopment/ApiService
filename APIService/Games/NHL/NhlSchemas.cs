using APIService.Config;

namespace APIService.Games.NHL;

public static class NhlSchemas
{
    public static readonly NhlSchema Nhl10 = new()
    {
        GamesTable = "games",
        Sources = new[]
        {
            new NhlReportSource { Mode = NhlReportMode.Vs, JsonKey = "VS", Table = "reports" }
        },
        Columns = new()
        {
            //nhl10 is weird and has team not home
            HomeIndicator = "team"
        },
        CacheKeyPrefix = "nhl10",
        UserHistoryStyle = NhlUserHistoryStyle.FullOrderedWithHitsAndShots,
        GameReportsIncludesGameIdField = true,
        Cache = new()
        {
            Players = TimeSpan.FromSeconds(30),
            Player = TimeSpan.FromSeconds(30),
            Games = TimeSpan.FromSeconds(30),
            Leaderboard = TimeSpan.FromSeconds(60),
            GlobalStats = TimeSpan.FromSeconds(60),
        }
    };

    public static readonly NhlSchema Nhl11 = new()
    {
        GamesTable = "games",
        Sources = new[]
        {
            new NhlReportSource { Mode = NhlReportMode.Vs,  JsonKey = "VS",  Table = "reports" },
            new NhlReportSource { Mode = NhlReportMode.So,  JsonKey = "SO",  Table = "so_reports" },
            new NhlReportSource { Mode = NhlReportMode.Otp, JsonKey = "OTP", Table = "otp_reportsl" },
        }
    };

    public static readonly NhlSchema Nhl12 = new()
    {
        GamesTable = "games_l",
        Sources = new[]
        {
            new NhlReportSource { Mode = NhlReportMode.Vs, JsonKey = "VS", Table = "reports_l" },
            new NhlReportSource { Mode = NhlReportMode.So, JsonKey = "SO", Table = "so_reports_l" },
        }
    };

    public static readonly NhlSchema Shared = new()
    {
        GamesTable = "games",
        Sources = new[]
        {
            new NhlReportSource { Mode = NhlReportMode.Vs, JsonKey = "VS", Table = "reports_vs" },
            new NhlReportSource { Mode = NhlReportMode.So, JsonKey = "SO", Table = "reports_so" },
        },
        Columns = new()
        {
            GamerTag = "gtag",
            Score = "scor",
            TeamName = "tnam",
            Shots = "shts",
        },
        RawGamesOrderedByCreatedAt = true,
        UserHistoryStyle = NhlUserHistoryStyle.GroupedFullUnordered,
        LowercasePlayerCacheKey = true,
        Cache = new()
        {
            Players = TimeSpan.FromSeconds(30),
            Player = TimeSpan.FromSeconds(30),
            Leaderboard = TimeSpan.FromSeconds(30),
        }
    };

    public static NhlSchema For(GameType type) => type switch
    {
        GameType.Nhl10 => Nhl10,
        GameType.Nhl11 => Nhl11,
        GameType.Nhl12 => Nhl12,
        GameType.Nhl13 or GameType.Nhl14 or GameType.Nhl15 or GameType.NhlLegacy => Shared,
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, "Not an NHL game type")
    };
}
