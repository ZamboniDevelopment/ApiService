using APIService.Config;
using APIService.Core;
using Microsoft.AspNetCore.Builder;
using Npgsql;
using System.Text.Json;

namespace APIService.Games.NHL14Legacy;

public static class NHLSharedApi
{
    public static void Map(WebApplication app, GameConfig game)
    {
        string prefix = "/" + game.RoutePrefix.Trim('/');

        /*
         * TODO: MISSING:
         * nhllegacy/status, nhl14/status
         */
        // GET | List of players (REDIS ROUTE)
        app.MapGet($"{prefix}/api/players", async (HttpContext ctx) =>
        {
            var redis = RedisUtils.GetDatabase(ctx);
            string key = $"nhl:{game.RoutePrefix}:players";

            if (redis != null)
            {
                var cached = await redis.StringGetAsync(key);
                if (cached.HasValue)
                    return Results.Text(cached!, "application/json");
            }

            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var rows = await DbUtils.ReadRows(conn, """
                                                        SELECT DISTINCT gtag FROM reports_vs
                                                        UNION
                                                        SELECT DISTINCT gtag FROM reports_so
                                                    """);

            var result = rows
                .Select(r => r["gtag"])
                .Where(x => x != null)
                .ToArray();

            var json = JsonSerializer.Serialize(result);

            if (redis != null)
                await redis.StringSetAsync(key, json, TimeSpan.FromSeconds(30));

            return Results.Text(json, "application/json");
        });

        // GET | Returns info about player via gamertag (REDIS SUPPORT)
        app.MapGet($"{prefix}/api/player/{{gamertag}}", async (HttpContext ctx, string gamertag) =>
        {
            var redis = RedisUtils.GetDatabase(ctx);
            string key = $"nhl:{game.RoutePrefix}:player:{gamertag.ToLowerInvariant()}";

            if (redis != null)
            {
                var cached = await redis.StringGetAsync(key);
                if (cached.HasValue)
                    return Results.Text(cached!, "application/json");
            }

            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var vs = await DbUtils.ReadRows(conn, """
                                                      SELECT user_id, scor
                                                      FROM reports_vs
                                                      WHERE gtag = @gt
                                                  """, new NpgsqlParameter("gt", gamertag));
            var so = await DbUtils.ReadRows(conn, """
                                                      SELECT user_id, scor
                                                      FROM reports_so
                                                      WHERE gtag = @gt
                                                  """, new NpgsqlParameter("gt", gamertag));
            if (!vs.Any() && !so.Any())
                return Results.NotFound();

            var userId =
                vs.FirstOrDefault()?["user_id"]
                ?? so.FirstOrDefault()?["user_id"];

            int vsGames = vs.Count;
            int soGames = so.Count;
            int vsGoals = vs.Sum(r => Convert.ToInt32(r["scor"] ?? 0));
            int soGoals = so.Sum(r => Convert.ToInt32(r["scor"] ?? 0));

            var result = new
            {
                userId = Convert.ToInt64(userId!),
                playerName = gamertag,
                VS = new
                {
                    games = vsGames,
                    goals = vsGoals
                },
                SO = new
                {
                    games = soGames,
                    goals = soGoals
                },
                totalGames = vsGames + soGames,
                totalGoals = vsGoals + soGoals
            };

            var json = JsonSerializer.Serialize(result);
            if (redis != null)
                await redis.StringSetAsync(key, json, TimeSpan.FromSeconds(30));
            return Results.Text(json, "application/json");
        });

        // GET | Returns raw games table data
        app.MapGet($"{prefix}/api/raw/games", async () =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();
            return Results.Json(await DbUtils.ReadRows(conn, "SELECT * FROM games ORDER BY created_at DESC"));
        });

        // GET | Returns raw reports table data
        app.MapGet($"{prefix}/api/raw/reports", async () =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var vs = await DbUtils.ReadRows(conn, "SELECT * FROM reports_vs ORDER BY created_at DESC");
            var so = await DbUtils.ReadRows(conn, "SELECT * FROM reports_so ORDER BY created_at DESC");

            return Results.Json(new
            {
                VS = vs ?? new(),
                SO = so ?? new()
            });
        });

        // GET | Returns a better list of games combined from reports
app.MapGet($"{prefix}/api/games", async () =>
{
    await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
    await conn.OpenAsync();

    var games = await DbUtils.ReadRows(
        conn,
        "SELECT * FROM games ORDER BY created_at DESC"
    );

    var vsReports = await DbUtils.ReadRows(conn, "SELECT * FROM reports_vs");
    var soReports = await DbUtils.ReadRows(conn, "SELECT * FROM reports_so");

    static long L(object? v)
        => v == null || v == DBNull.Value ? 0L : Convert.ToInt64(v);

    var vsByGame = vsReports
        .GroupBy(r => Convert.ToInt64(r["game_id"]))
        .ToDictionary(g => g.Key, g => g.ToList());

    var soByGame = soReports
        .GroupBy(r => Convert.ToInt64(r["game_id"]))
        .ToDictionary(g => g.Key, g => g.ToList());

    var vsGames = new List<object>();
    var soGames = new List<object>();

    foreach (var g in games)
    {
        var id = Convert.ToInt64(g["game_id"]);

        if (vsByGame.TryGetValue(id, out var reps))
        {
            vsGames.Add(new
            {
                game_id = id,
                created_at = g["created_at"],
                fnsh = g.GetValueOrDefault("fnsh"),
                gtyp = g.GetValueOrDefault("gtyp"),
                venue = g.GetValueOrDefault("venue"),

                players = reps.Count,
                totalGoals = reps.Sum(r => L(r["scor"])),
                avgFps = reps.Any() ? reps.Average(r => L(r["fpsavg"])) : 0,
                avgLatency = reps.Any() ? reps.Average(r => L(r["lateavgnet"])) : 0,

                teams = reps.Select(r => new
                {
                    team_name = r.GetValueOrDefault("tnam"),
                    score = r.GetValueOrDefault("scor"),
                    shots = r.GetValueOrDefault("shts"),
                    hits = r.GetValueOrDefault("hits"),
                    gamertag = r.GetValueOrDefault("gtag")
                }),

                status = Convert.ToBoolean(g.GetValueOrDefault("fnsh") ?? false)
                    ? "Finished"
                    : "In Progress"
            });
        }

        if (soByGame.TryGetValue(id, out reps))
        {
            soGames.Add(new
            {
                game_id = id,
                created_at = g["created_at"],
                fnsh = g.GetValueOrDefault("fnsh"),
                gtyp = g.GetValueOrDefault("gtyp"),
                venue = g.GetValueOrDefault("venue"),

                players = reps.Count,
                totalGoals = reps.Sum(r => L(r["scor"])),
                avgFps = reps.Any() ? reps.Average(r => L(r["fpsavg"])) : 0,
                avgLatency = reps.Any() ? reps.Average(r => L(r["lateavgnet"])) : 0,

                teams = reps.Select(r => new
                {
                    team_name = r.GetValueOrDefault("tnam"),
                    score = r.GetValueOrDefault("scor"),
                    shots = r.GetValueOrDefault("shts"),
                    hits = r.GetValueOrDefault("hits"),
                    gamertag = r.GetValueOrDefault("gtag")
                }),

                status = Convert.ToBoolean(g.GetValueOrDefault("fnsh") ?? false)
                    ? "Finished"
                    : "In Progress"
            });
        }
    }

    return Results.Json(new
    {
        VS = vsGames,
        SO = soGames
    });
});
        // GET | Returns VS or SO reports via game id
        app.MapGet($"{prefix}/api/game/{{id:int}}/reports", async (int id) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var vs = await DbUtils.ReadRows(
                conn,
                "SELECT * FROM reports_vs WHERE game_id=@id",
                new NpgsqlParameter("id", id)
            );

            var so = await DbUtils.ReadRows(
                conn,
                "SELECT * FROM reports_so WHERE game_id=@id",
                new NpgsqlParameter("id", id)
            );

            return Results.Json(new
            {
                VS = vs ?? new List<Dictionary<string, object?>>(),
                SO = so ?? new List<Dictionary<string, object?>>()
            });
        });

        // GET | Returns summary from summary of game via id
        app.MapGet($"{prefix}/api/games/{{id:int}}/summary", async (int id) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var vs = await DbUtils.ReadRows(
                conn,
                "SELECT * FROM reports_vs WHERE game_id=@id",
                new NpgsqlParameter("id", id)
            );

            var so = await DbUtils.ReadRows(
                conn,
                "SELECT * FROM reports_so WHERE game_id=@id",
                new NpgsqlParameter("id", id)
            );

            var all = vs.Concat(so).ToList();

            if (!all.Any())
                return Results.NotFound(); // no reports at all

            var home = all.Where(r => Convert.ToBoolean(r["home"])).ToList();
            var away = all.Where(r => !Convert.ToBoolean(r["home"])).ToList();

            int homeScore = home.Sum(r => Convert.ToInt32(r["scor"] ?? 0));
            int awayScore = away.Sum(r => Convert.ToInt32(r["scor"] ?? 0));

            return Results.Json(new
            {
                gameId = id,
                homeScore,
                awayScore,
                VS = vs,
                SO = so
            });
        });

        // GET | Returns leaderboards by spesified range (REDIS SUPPORT)
        app.MapGet($"{prefix}/api/leaderboard/{{range}}", async (HttpContext ctx, string range) =>
        {
            var redis = RedisUtils.GetDatabase(ctx);
            string key = $"nhl:{game.RoutePrefix}:leaderboard:{range}";

            if (redis != null)
            {
                var cached = await redis.StringGetAsync(key);
                if (cached.HasValue)
                    return Results.Text(cached!, "application/json");
            }

            var from = DbUtils.RangeToDate(range);

            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var sql = """
                          SELECT gtag, SUM(scor) AS total_goals, COUNT(*) AS games_played
                          FROM (
                              SELECT gtag, scor, created_at FROM reports_vs
                              UNION ALL
                              SELECT gtag, scor, created_at FROM reports_so
                          ) x
                          WHERE (@from = '0001-01-01'::timestamp OR created_at >= @from)
                          GROUP BY gtag
                          ORDER BY total_goals DESC
                      """;

            using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("from", from);

            var list = new List<object>();
            using var r = await cmd.ExecuteReaderAsync();

            int rank = 1;
            while (await r.ReadAsync())
            {
                list.Add(new
                {
                    gamertag = r["gtag"],
                    totalGoals = r["total_goals"],
                    gamesPlayed = r["games_played"],
                    rank = rank++
                });
            }

            var json = JsonSerializer.Serialize(list);

            if (redis != null)
                await redis.StringSetAsync(key, json, TimeSpan.FromSeconds(30));

            return Results.Text(json, "application/json");
        });

        // GET | Returns global stats
        app.MapGet($"{prefix}/api/stats/global", async () =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var games = Convert.ToInt32(
                (await new NpgsqlCommand("SELECT COUNT(*) FROM games", conn).ExecuteScalarAsync())!);
            var reports =
                Convert.ToInt32(
                    (await new NpgsqlCommand("SELECT COUNT(*) FROM reports_vs", conn).ExecuteScalarAsync())!) +
                Convert.ToInt32(
                    (await new NpgsqlCommand("SELECT COUNT(*) FROM reports_so", conn).ExecuteScalarAsync())!);

            var players = Convert.ToInt32((await new NpgsqlCommand("""
                                                                       SELECT COUNT(DISTINCT gtag) FROM (
                                                                           SELECT gtag FROM reports_vs
                                                                           UNION
                                                                           SELECT gtag FROM reports_so
                                                                       ) x
                                                                   """, conn).ExecuteScalarAsync())!);

            return Results.Json(new
            {
                totalGames = games,
                totalReports = reports,
                totalPlayers = players
            });
        });

        // GET | Returns latest reports with limit
        app.MapGet($"{prefix}/api/reports/latest", async (int? limit) =>
        {
            int max = Math.Clamp(limit ?? 50, 1, 500);

            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            return Results.Json(await DbUtils.ReadRows(conn, $"""
                                                                  SELECT * FROM (
                                                                      SELECT * FROM reports_vs
                                                                      UNION ALL
                                                                      SELECT * FROM reports_so
                                                                  ) x
                                                                  ORDER BY created_at DESC
                                                                  LIMIT {max}
                                                              """));
        });

        // GET | Returns users history of games from reports via id
        app.MapGet($"{prefix}/api/user/{{id:long}}/history", async (long id) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var vs = await DbUtils.ReadRows(conn,
                "SELECT * FROM reports_vs WHERE user_id=@id",
                new NpgsqlParameter("id", id));

            var so = await DbUtils.ReadRows(conn,
                "SELECT * FROM reports_so WHERE user_id=@id",
                new NpgsqlParameter("id", id));

            if (!vs.Any() && !so.Any())
                return Results.Json(Array.Empty<object>());

            var userReports = vs.Concat(so).ToList();
            var gameIds = userReports.Select(r => Convert.ToInt64(r["game_id"])).Distinct().ToArray();

            var oppVs = await DbUtils.ReadRows(conn,
                "SELECT * FROM reports_vs WHERE game_id = ANY(@ids)",
                new NpgsqlParameter("ids", gameIds));

            var oppSo = await DbUtils.ReadRows(conn,
                "SELECT * FROM reports_so WHERE game_id = ANY(@ids)",
                new NpgsqlParameter("ids", gameIds));

            var oppAll = oppVs.Concat(oppSo).ToList();

            foreach (var r in userReports)
            {
                var opp = oppAll.FirstOrDefault(o =>
                    Convert.ToInt64(o["game_id"]) == Convert.ToInt64(r["game_id"]) &&
                    Convert.ToInt64(o["user_id"]) != Convert.ToInt64(r["user_id"])
                );

                if (opp != null)
                {
                    r["opponent"] = opp["gtag"];
                    r["opponent_team"] = opp["tnam"];
                    r["opponent_score"] = opp["scor"];
                }
            }

            return Results.Json(new
            {
                VS = vs,
                SO = so
            });
        });
    }
}