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

        // GET | Returns raw games table data
        app.MapGet($"{prefix}/api/raw/games", async () =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();
            return Results.Json(await DbUtils.ReadRows(conn, "SELECT * FROM games ORDER BY created_at DESC"));
        });

        // GET | Returns raw reports table data
        // BUG: UNION ALL MISMATCH TABLE (UNMATCHED DATA)
        app.MapGet($"{prefix}/api/raw/reports", async () =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            return Results.Json(await DbUtils.ReadRows(conn, """
                SELECT * FROM reports_vs
                UNION ALL
                SELECT * FROM reports_so
                ORDER BY created_at DESC
            """));
        });

        // GET | Returns a better list of games combined from reports
        // BUG: UNION ALL MISMATCH TABLE (UNMATCHED DATA)
        app.MapGet($"{prefix}/api/games", async () =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var games = await DbUtils.ReadRows(conn, "SELECT * FROM games ORDER BY created_at DESC");

            var reports = await DbUtils.ReadRows(conn, """
                SELECT * FROM reports_vs
                UNION ALL
                SELECT * FROM reports_so
            """);

            var grouped = reports
                .GroupBy(r => Convert.ToInt64(r["game_id"]))
                .ToDictionary(g => g.Key, g => g.ToList());

            var result = new List<object>();

            foreach (var g in games)
            {
                var id = Convert.ToInt64(g["game_id"]);
                grouped.TryGetValue(id, out var reps);
                reps ??= new();

                result.Add(new
                {
                    game_id = id,
                    created_at = g["created_at"],
                    teams = reps.Select(r => r["team_name"]).Distinct(),
                    players = reps.Count,
                    totalGoals = reps.Sum(r => Convert.ToInt32(r["scor"] ?? 0)),
                    avgFps = reps.Any() ? reps.Average(r => Convert.ToInt32(r["fpsavg"] ?? 0)) : 0,
                    avgLatency = reps.Any() ? reps.Average(r => Convert.ToInt32(r["lateavgnet"] ?? 0)) : 0,
                    status = reps.Count > 0 ? "Finished" : "Unknown"
                });
            }

            return Results.Json(result);
        });

        // GET | Returns VS or SO reports via game id
        app.MapGet($"{prefix}/api/game/{{id:int}}/reports", async (int id) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            return Results.Json(await DbUtils.ReadRows(conn, """
                SELECT * FROM reports_vs WHERE game_id=@id
                UNION ALL
                SELECT * FROM reports_so WHERE game_id=@id
            """, new NpgsqlParameter("id", id)));
        });

        // GET | Returns summary from summary of game via id
        // BUG: UNION ALL MISMATCH TABLE (UNMATCHED DATA)
        app.MapGet($"{prefix}/api/games/{{id:int}}/summary", async (int id) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var reports = await DbUtils.ReadRows(conn, """
                SELECT * FROM reports_vs WHERE game_id=@id
                UNION ALL
                SELECT * FROM reports_so WHERE game_id=@id
            """, new NpgsqlParameter("id", id));

            if (reports.Count == 0)
                return Results.NotFound();

            var home = reports.Where(r => Convert.ToBoolean(r["home"])).ToList();
            var away = reports.Where(r => !Convert.ToBoolean(r["home"])).ToList();

            int homeScore = home.Sum(r => Convert.ToInt32(r["scor"] ?? 0));
            int awayScore = away.Sum(r => Convert.ToInt32(r["scor"] ?? 0));

            string? winner =
                homeScore > awayScore ? home.FirstOrDefault()?["team_name"]?.ToString() :
                awayScore > homeScore ? away.FirstOrDefault()?["team_name"]?.ToString() :
                null;

            return Results.Json(new
            {
                gameId = id,
                homeTeam = home.FirstOrDefault()?["team_name"],
                awayTeam = away.FirstOrDefault()?["team_name"],
                homeScore,
                awayScore,
                winnerTeam = winner,
                reports
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

            var games = Convert.ToInt32((await new NpgsqlCommand("SELECT COUNT(*) FROM games", conn).ExecuteScalarAsync())!);
            var reports =
                Convert.ToInt32((await new NpgsqlCommand("SELECT COUNT(*) FROM reports_vs", conn).ExecuteScalarAsync())!) +
                Convert.ToInt32((await new NpgsqlCommand("SELECT COUNT(*) FROM reports_so", conn).ExecuteScalarAsync())!);

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

            var userRows = await DbUtils.ReadRows(conn, """
                SELECT * FROM reports_vs WHERE user_id=@id
                UNION ALL
                SELECT * FROM reports_so WHERE user_id=@id
            """, new NpgsqlParameter("id", id));

            if (userRows.Count == 0)
                return Results.Json(Array.Empty<object>());

            var gameIds = userRows.Select(r => Convert.ToInt64(r["game_id"])).Distinct().ToArray();

            var oppRows = await DbUtils.ReadRows(conn, """
                SELECT * FROM reports_vs WHERE game_id = ANY(@ids)
                UNION ALL
                SELECT * FROM reports_so WHERE game_id = ANY(@ids)
            """, new NpgsqlParameter("ids", gameIds));

            foreach (var r in userRows)
            {
                var opp = oppRows.FirstOrDefault(o =>
                    Convert.ToInt64(o["game_id"]) == Convert.ToInt64(r["game_id"]) &&
                    Convert.ToInt64(o["user_id"]) != Convert.ToInt64(r["user_id"])
                );

                if (opp != null)
                {
                    r["opponent"] = opp["gtag"];
                    r["opponent_team"] = opp["team_name"];
                    r["opponent_score"] = opp["scor"];
                }
            }

            return Results.Json(userRows);
        });
    }
}