using APIService.Config;
using APIService.Core;
using Npgsql;
using System.Text.Json;

namespace APIService.Games.NHL;

// Shared route registration + data access for every NHL variant
public static class NhlEndpoints
{
    public static void Map(WebApplication app, GameConfig game, NhlSchema schema)
    {
        string prefix = "/" + game.RoutePrefix.Trim('/');

        MapPlayers(app, game, schema, prefix);
        MapPlayer(app, game, schema, prefix);
        MapGames(app, game, schema, prefix);
        MapGameReports(app, game, schema, prefix);
        MapGameSummary(app, game, schema, prefix);
        MapLeaderboard(app, game, schema, prefix);
        MapStatsGlobal(app, game, schema, prefix);
        MapReportsLatest(app, game, schema, prefix);
        MapUserHistory(app, game, schema, prefix);
        MapRawGames(app, game, schema, prefix);
        MapRawReports(app, game, schema, prefix);
    }

    // GET /api/players
    private static void MapPlayers(WebApplication app, GameConfig game, NhlSchema schema, string prefix)
    {
        app.MapGet($"{prefix}/api/players", async (HttpContext ctx) =>
        {
            var redis = RedisUtils.GetDatabase(ctx);
            string key = $"{schema.CacheKeyPrefix}:{game.RoutePrefix}:players";

            var json = await CacheHelper.GetOrComputeJsonAsync(redis, key, schema.Cache.Players, async () =>
            {
                await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
                await conn.OpenAsync();

                string sql = string.Join(" UNION ",
                    schema.Sources.Select(s => $"SELECT DISTINCT {schema.Columns.GamerTag} FROM {s.Table}"));

                var rows = await DbUtils.ReadRows(conn, sql);

                var result = rows
                    .Select(r => r[schema.Columns.GamerTag])
                    .Where(x => x != null)
                    .ToArray();

                return JsonSerializer.Serialize(result);
            });

            return Results.Text(json, "application/json");
        });
    }

    // GET /api/player/{gamertag}
    private static void MapPlayer(WebApplication app, GameConfig game, NhlSchema schema, string prefix)
    {
        app.MapGet($"{prefix}/api/player/{{gamertag}}", async (HttpContext ctx, string gamertag) =>
        {
            var redis = RedisUtils.GetDatabase(ctx);
            string cacheGamertag = schema.LowercasePlayerCacheKey ? gamertag.ToLowerInvariant() : gamertag;
            string key = $"{schema.CacheKeyPrefix}:{game.RoutePrefix}:player:{cacheGamertag}";
            
            string json = await CacheHelper.GetOrComputeJsonAsync(redis, key, schema.Cache.Player, async () =>
            {
                await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
                await conn.OpenAsync();

                var perSource = new List<(string JsonKey, List<Dictionary<string, object?>> Rows)>();
                foreach (var s in schema.Sources)
                {
                    var rows = await DbUtils.ReadRows(conn,
                        $"SELECT user_id, {schema.Columns.Score} AS score FROM {s.Table} WHERE {schema.Columns.GamerTag}=@gt",
                        new NpgsqlParameter("gt", gamertag));
                    perSource.Add((s.JsonKey, rows));
                }

                if (perSource.All(s => s.Rows.Count == 0))
                    return "";

                var userId = perSource.SelectMany(s => s.Rows).First()["user_id"];
                var (perSourceCounts, totalGames, totalGoals) = NhlSummaryLogic.ComputePlayerBreakdown(perSource);

                var breakdown = perSourceCounts.ToDictionary(
                    kv => kv.Key,
                    kv => (object)new { games = kv.Value.Games, goals = kv.Value.Goals });

                var result = new
                {
                    userId,
                    playerName = gamertag,
                    breakdown,
                    totalGames,
                    totalGoals
                };

                return JsonSerializer.Serialize(result);
            });

            return json.Length == 0 ? Results.NotFound() : Results.Text(json, "application/json");
        });
    }

    // GET /api/games
    private static void MapGames(WebApplication app, GameConfig game, NhlSchema schema, string prefix)
    {
        app.MapGet($"{prefix}/api/games", async (HttpContext ctx) =>
        {
            var redis = RedisUtils.GetDatabase(ctx);
            string key = $"{schema.CacheKeyPrefix}:{game.RoutePrefix}:games";

            var json = await CacheHelper.GetOrComputeJsonAsync(redis, key, schema.Cache.Games, async () =>
            {
                await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
                await conn.OpenAsync();

                var games = await DbUtils.ReadRows(conn, $"SELECT * FROM {schema.GamesTable} ORDER BY created_at DESC");

                var byMode = new Dictionary<string, List<object>>();
                foreach (var s in schema.Sources)
                {
                    var reports = await DbUtils.ReadRows(conn, $"SELECT * FROM {s.Table}");
                    var grouped = reports
                        .GroupBy(r => Helper.L(r["game_id"]))
                        .ToDictionary(g => g.Key, g => g.ToList());

                    byMode[s.JsonKey] = games
                        .Where(g => grouped.ContainsKey(Helper.L(g["game_id"])))
                        .Select(g => BuildGameSummary(g, grouped[Helper.L(g["game_id"])], schema.Columns))
                        .ToList();
                }

                object payload = schema.Sources.Count == 1
                    ? byMode.Single().Value
                    : byMode;

                return JsonSerializer.Serialize(payload);
            });

            return Results.Text(json, "application/json");
        });
    }

    private static object BuildGameSummary(
        Dictionary<string, object?> g, List<Dictionary<string, object?>> reps, NhlColumns cols)
    {
        const double MaxLatencyCap = 1_000_000.0;
        return new
        {
            game_id = g.GetValueOrDefault("game_id"),
            created_at = g.GetValueOrDefault("created_at"),
            fnsh = g.GetValueOrDefault("fnsh"),
            gtyp = g.GetValueOrDefault("gtyp"),
            venue = g.GetValueOrDefault("venue"),
            players = reps.Count,
            totalGoals = reps.Sum(r => Helper.I(r[cols.Score])),
        
            avgFps = reps.Count > 0 
                ? Math.Round(reps.Average(r => Convert.ToDouble(r["fpsavg"] ?? 0)), 2) 
                : 0.0,

            // ea moment
            avgLatency = reps.Count > 0 
                ? Math.Round(reps.Average(r => 
                    Math.Min(Convert.ToDouble(r["lateavgnet"] ?? 0), MaxLatencyCap)), 2) 
                : 0.0,
            avgTeamLatency = reps.Count > 0 
                ? Math.Round(reps.Average(r => 
                    Math.Min(Convert.ToDouble(r["ltean"] ?? r["ltennet"] ?? 0), MaxLatencyCap)), 2) 
                : 0.0,

            teams = reps.Select(r => new
            {
                team_name = r.GetValueOrDefault(cols.TeamName),
                score = r.GetValueOrDefault(cols.Score),
                shots = r.GetValueOrDefault(cols.Shots),
                hits = r.GetValueOrDefault("hits"),
                gamertag = r.GetValueOrDefault(cols.GamerTag),
                netLatency = Math.Min(Convert.ToDouble(r.GetValueOrDefault("lateavgnet") ?? 0), MaxLatencyCap),
                teamLatency = Math.Min(Convert.ToDouble(r.GetValueOrDefault("ltean") ?? r.GetValueOrDefault("ltennet") ?? 0), MaxLatencyCap)
            }),
            status = Convert.ToBoolean(g.GetValueOrDefault("fnsh") ?? false) ? "Finished" : "In Progress"
        };
    }
    
    // GET /api/game/{id:long}/reports
    private static void MapGameReports(WebApplication app, GameConfig game, NhlSchema schema, string prefix)
    {
        app.MapGet($"{prefix}/api/game/{{id:long}}/reports", async (long id) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            if (schema.GameReportsIncludesGameIdField)
            {
                var rows = await DbUtils.ReadRows(conn,
                    $"SELECT user_id, {schema.Columns.GamerTag}, {schema.Columns.Score} " +
                    $"FROM {schema.Sources[0].Table} WHERE game_id = @id",
                    new NpgsqlParameter("id", id));

                return Results.Json(new { gameId = id, reports = rows });
            }

            var result = new Dictionary<string, object>();
            foreach (var s in schema.Sources)
                result[s.JsonKey] = await DbUtils.ReadRows(conn,
                    $"SELECT * FROM {s.Table} WHERE game_id=@id", new NpgsqlParameter("id", id));

            return Results.Json(result);
        });
    }

    // GET /api/games/{id:long}/summary
    private static void MapGameSummary(WebApplication app, GameConfig game, NhlSchema schema, string prefix)
    {
        app.MapGet($"{prefix}/api/games/{{id:long}}/summary", async (long id) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var gameRows = await DbUtils.ReadRows(conn,
                $"SELECT game_id, created_at FROM {schema.GamesTable} WHERE game_id=@id",
                new NpgsqlParameter("id", id));

            if (gameRows.Count == 0)
                return Results.NotFound();

            var playedAt = gameRows[0]["created_at"];

            var perSource = new List<(string JsonKey, List<Dictionary<string, object?>> Rows)>();
            foreach (var s in schema.Sources)
            {
                var rows = await DbUtils.ReadRows(conn, $"""
                    SELECT user_id,
                           {schema.Columns.GamerTag} AS gamertag,
                           {schema.Columns.HomeIndicator} AS home_ind,
                           {schema.Columns.TeamName} AS team_name,
                           {schema.Columns.Score} AS score
                    FROM {s.Table}
                    WHERE game_id=@id
                    """, new NpgsqlParameter("id", id));
                perSource.Add((s.JsonKey, rows));
            }

            var allRows = perSource.SelectMany(s => s.Rows).ToList();
            var summary = NhlSummaryLogic.ComputeGameSummary(allRows);

            object BuildReportView(Dictionary<string, object?> r) => new
            {
                userId = r["user_id"],
                gamertag = r["gamertag"],
                teamName = r["team_name"],
                score = r["score"]
            };

            object reportsField = schema.Sources.Count == 1
                ? perSource[0].Rows.Select(BuildReportView).ToList()
                : perSource.ToDictionary(s => s.JsonKey, s => (object)s.Rows.Select(BuildReportView).ToList());

            return Results.Json(new
            {
                gameId = id,
                playedAt,
                homeTeam = summary.HomeTeam,
                awayTeam = summary.AwayTeam,
                homeScore = summary.HomeScore,
                awayScore = summary.AwayScore,
                winnerTeam = summary.WinnerTeam,
                reports = reportsField
            });
        });
    }

    // GET /api/leaderboard/{range}
    private static void MapLeaderboard(WebApplication app, GameConfig game, NhlSchema schema, string prefix)
    {
        app.MapGet($"{prefix}/api/leaderboard/{{range}}", async (HttpContext ctx, string range) =>
        {
            var redis = RedisUtils.GetDatabase(ctx);
            string key = $"{schema.CacheKeyPrefix}:{game.RoutePrefix}:leaderboard:{range}";

            var json = await CacheHelper.GetOrComputeJsonAsync(redis, key, schema.Cache.Leaderboard, async () =>
            {
                var from = DbUtils.RangeToDate(range);

                await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
                await conn.OpenAsync();

                string sql;
                if (schema.Sources.Count == 1)
                {
                    sql = $"""
                        SELECT r.{schema.Columns.GamerTag} AS gamertag, SUM(r.{schema.Columns.Score}) AS total_goals, COUNT(*) AS games_played
                        FROM {schema.Sources[0].Table} r
                        JOIN {schema.GamesTable} g ON g.game_id = r.game_id
                        WHERE (@from = '0001-01-01'::timestamp OR g.created_at >= @from)
                        GROUP BY r.{schema.Columns.GamerTag}
                        ORDER BY total_goals DESC
                        """;
                }
                else
                {
                    string union = string.Join(" UNION ALL ", schema.Sources.Select(s =>
                        $"SELECT {schema.Columns.GamerTag} AS gamertag, {schema.Columns.Score} AS score, created_at FROM {s.Table}"));

                    sql = $"""
                        SELECT gamertag, SUM(score) AS total_goals, COUNT(*) AS games_played
                        FROM ({union}) x
                        WHERE (@from = '0001-01-01'::timestamp OR created_at >= @from)
                        GROUP BY gamertag
                        ORDER BY total_goals DESC
                        """;
                }

                using var cmd = new NpgsqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("from", from);

                var list = new List<object>();
                int rank = 1;

                using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    list.Add(new
                    {
                        gamertag = reader["gamertag"],
                        totalGoals = reader["total_goals"],
                        gamesPlayed = reader["games_played"],
                        rank = rank++
                    });
                }

                return JsonSerializer.Serialize(list);
            });

            return Results.Text(json, "application/json");
        });
    }

    // GET /api/stats/global
    private static void MapStatsGlobal(WebApplication app, GameConfig game, NhlSchema schema, string prefix)
    {
        app.MapGet($"{prefix}/api/stats/global", async (HttpContext ctx) =>
        {
            var redis = RedisUtils.GetDatabase(ctx);
            string key = $"{schema.CacheKeyPrefix}:{game.RoutePrefix}:stats:global";

            var json = await CacheHelper.GetOrComputeJsonAsync(redis, key, schema.Cache.GlobalStats, async () =>
            {
                await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
                await conn.OpenAsync();

                int totalGames = Convert.ToInt32(
                    await new NpgsqlCommand($"SELECT COUNT(*) FROM {schema.GamesTable}", conn).ExecuteScalarAsync());

                int totalReports, totalPlayers;

                if (schema.Sources.Count == 1)
                {
                    var table = schema.Sources[0].Table;
                    totalReports = Convert.ToInt32(
                        await new NpgsqlCommand($"SELECT COUNT(*) FROM {table}", conn).ExecuteScalarAsync());
                    totalPlayers = Convert.ToInt32(
                        await new NpgsqlCommand($"SELECT COUNT(DISTINCT {schema.Columns.GamerTag}) FROM {table}", conn)
                            .ExecuteScalarAsync());
                }
                else
                {
                    string reportsUnion = string.Join(" UNION ALL ",
                        schema.Sources.Select(s => $"SELECT game_id FROM {s.Table}"));
                    totalReports = Convert.ToInt32(
                        await new NpgsqlCommand($"SELECT COUNT(*) FROM ({reportsUnion}) x", conn).ExecuteScalarAsync());

                    string playersUnion = string.Join(" UNION ",
                        schema.Sources.Select(s => $"SELECT {schema.Columns.GamerTag} FROM {s.Table}"));
                    totalPlayers = Convert.ToInt32(
                        await new NpgsqlCommand($"SELECT COUNT(DISTINCT {schema.Columns.GamerTag}) FROM ({playersUnion}) x", conn)
                            .ExecuteScalarAsync());
                }

                var payload = new { totalGames, totalReports, totalPlayers };
                return JsonSerializer.Serialize(payload);
            });

            return Results.Text(json, "application/json");
        });
    }

    // GET /api/reports/latest?limit=
    private static void MapReportsLatest(WebApplication app, GameConfig game, NhlSchema schema, string prefix)
    {
        app.MapGet($"{prefix}/api/reports/latest", async (int? limit) =>
        {
            int max = Math.Clamp(limit ?? 50, 1, 500);

            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            string sql;
            if (schema.Sources.Count == 1)
            {
                sql = $"SELECT * FROM {schema.Sources[0].Table} ORDER BY created_at DESC LIMIT {max}";
            }
            else
            {
                string union = string.Join("\nUNION ALL\n", schema.Sources.Select(s =>
                    $"SELECT game_id, user_id, {schema.Columns.GamerTag}, name, team, {schema.Columns.TeamName} AS team_name, " +
                    $"{schema.Columns.Score} AS score, home, quit, created_at, '{s.JsonKey}' AS report_type FROM {s.Table}"));

                sql = $"""
                    SELECT * FROM (
                    {union}
                    ) x
                    ORDER BY created_at DESC
                    LIMIT {max}
                    """;
            }

            return Results.Json(await DbUtils.ReadRows(conn, sql));
        });
    }

    // GET /api/user/{id:long}/history
    private static void MapUserHistory(WebApplication app, GameConfig game, NhlSchema schema, string prefix)
    {
        app.MapGet($"{prefix}/api/user/{{id:long}}/history", async (long id) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            return schema.UserHistoryStyle switch
            {
                NhlUserHistoryStyle.FullOrderedWithHitsAndShots =>
                    await UserHistoryFullOrdered(conn, schema, id),

                NhlUserHistoryStyle.ReducedFlatUnordered =>
                    await UserHistoryReducedFlat(conn, schema, id),

                NhlUserHistoryStyle.GroupedFullUnordered =>
                    await UserHistoryGroupedFull(conn, schema, id),

                _ => throw new ArgumentOutOfRangeException(nameof(schema.UserHistoryStyle))
            };
        });
    }
    
    private static async Task<IResult> UserHistoryFullOrdered(NpgsqlConnection conn, NhlSchema schema, long id)
    {
        var table = schema.Sources[0].Table;

        var userRows = await DbUtils.ReadRows(conn,
            $"SELECT * FROM {table} WHERE user_id=@id ORDER BY created_at DESC",
            new NpgsqlParameter("id", id));

        if (userRows.Count == 0)
            return Results.Json(Array.Empty<object>());

        var gameIds = userRows.Select(r => Helper.L(r["game_id"])).ToArray();

        var oppRows = await DbUtils.ReadRows(conn, $"""
            SELECT * FROM {table}
            WHERE game_id = ANY(@ids) AND user_id != @uid
            """,
            new NpgsqlParameter("ids", gameIds),
            new NpgsqlParameter("uid", id));

        foreach (var r in userRows)
        {
            var opp = oppRows.FirstOrDefault(o => Helper.L(o["game_id"]) == Helper.L(r["game_id"]));
            if (opp != null)
            {
                r["opponent"] = opp[schema.Columns.GamerTag];
                r["opponent_team"] = opp[schema.Columns.TeamName];
                r["opponent_score"] = opp[schema.Columns.Score];
                r["opponent_hits"] = opp.GetValueOrDefault("hits");
                r["opponent_shots"] = opp.GetValueOrDefault(schema.Columns.Shots);
            }
        }

        return Results.Json(userRows);
    }
    
    private static async Task<IResult> UserHistoryReducedFlat(NpgsqlConnection conn, NhlSchema schema, long id)
    {
        string cols = $"game_id, user_id, {schema.Columns.GamerTag}, {schema.Columns.TeamName}, {schema.Columns.Score}, created_at";

        string userSql = string.Join(" UNION ALL ",
            schema.Sources.Select(s => $"SELECT {cols} FROM {s.Table} WHERE user_id=@id"));
        var userRows = await DbUtils.ReadRows(conn, userSql, new NpgsqlParameter("id", id));

        if (userRows.Count == 0)
            return Results.Json(Array.Empty<object>());

        var gameIds = userRows.Select(r => Helper.L(r["game_id"])).Distinct().ToArray();

        string allSql = string.Join(" UNION ALL ",
            schema.Sources.Select(s => $"SELECT game_id, user_id, {schema.Columns.GamerTag}, {schema.Columns.TeamName}, {schema.Columns.Score} FROM {s.Table} WHERE game_id = ANY(@ids)"));
        var allRows = await DbUtils.ReadRows(conn, allSql, new NpgsqlParameter("ids", gameIds));

        foreach (var r in userRows)
        {
            var opp = allRows.FirstOrDefault(o =>
                Helper.L(o["game_id"]) == Helper.L(r["game_id"]) &&
                Helper.L(o["user_id"]) != Helper.L(r["user_id"]));

            if (opp != null)
            {
                r["opponent"] = opp[schema.Columns.GamerTag];
                r["opponent_team"] = opp[schema.Columns.TeamName];
                r["opponent_score"] = opp[schema.Columns.Score];
            }
        }

        return Results.Json(userRows);
    }
    
    private static async Task<IResult> UserHistoryGroupedFull(NpgsqlConnection conn, NhlSchema schema, long id)
    {
        var perSource = new List<(string JsonKey, List<Dictionary<string, object?>> Rows)>();
        foreach (var s in schema.Sources)
        {
            var rows = await DbUtils.ReadRows(conn,
                $"SELECT * FROM {s.Table} WHERE user_id=@id", new NpgsqlParameter("id", id));
            perSource.Add((s.JsonKey, rows));
        }

        if (perSource.All(s => s.Rows.Count == 0))
            return Results.Json(Array.Empty<object>());

        var userReports = perSource.SelectMany(s => s.Rows).ToList();
        var gameIds = userReports.Select(r => Helper.L(r["game_id"])).Distinct().ToArray();

        var oppAll = new List<Dictionary<string, object?>>();
        foreach (var s in schema.Sources)
        {
            var rows = await DbUtils.ReadRows(conn,
                $"SELECT * FROM {s.Table} WHERE game_id = ANY(@ids)", new NpgsqlParameter("ids", gameIds));
            oppAll.AddRange(rows);
        }

        foreach (var r in userReports)
        {
            var opp = oppAll.FirstOrDefault(o =>
                Helper.L(o["game_id"]) == Helper.L(r["game_id"]) &&
                Helper.L(o["user_id"]) != Helper.L(r["user_id"]));

            if (opp != null)
            {
                r["opponent"] = opp[schema.Columns.GamerTag];
                r["opponent_team"] = opp[schema.Columns.TeamName];
                r["opponent_score"] = opp[schema.Columns.Score];
            }
        }

        var grouped = perSource.ToDictionary(s => s.JsonKey, s => (object)s.Rows);
        return Results.Json(grouped);
    }

    // GET /api/raw/games
    private static void MapRawGames(WebApplication app, GameConfig game, NhlSchema schema, string prefix)
    {
        app.MapGet($"{prefix}/api/raw/games", async () =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            string sql = schema.RawGamesOrderedByCreatedAt
                ? $"SELECT * FROM {schema.GamesTable} ORDER BY created_at DESC"
                : $"SELECT * FROM {schema.GamesTable}";

            return Results.Json(await DbUtils.ReadRows(conn, sql));
        });
    }

    // GET /api/raw/reports
    private static void MapRawReports(WebApplication app, GameConfig game, NhlSchema schema, string prefix)
    {
        app.MapGet($"{prefix}/api/raw/reports", async () =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            if (schema.Sources.Count == 1)
                return Results.Json(await DbUtils.ReadRows(conn, $"SELECT * FROM {schema.Sources[0].Table}"));

            var result = new Dictionary<string, object>();
            foreach (var s in schema.Sources)
                result[s.JsonKey] = await DbUtils.ReadRows(conn, $"SELECT * FROM {s.Table} ORDER BY created_at DESC");

            return Results.Json(result);
        });
    }
}
