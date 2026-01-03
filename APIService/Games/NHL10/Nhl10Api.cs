using APIService.Config;
using APIService.Core;
using Npgsql;
using System.Text.Json;

namespace APIService.Games.NHL10;

public static class Nhl10Api
{
    public static void Map(WebApplication app, GameConfig game)
    {
        string prefix = "/" + game.RoutePrefix.Trim('/');
        
        /*
         * TODO: MISSING:
         * nhl10/api/stats/active
         * nhl10/status
         */
        
        // PLAYERS
        app.MapGet($"{prefix}/api/players", async (HttpContext ctx) =>
        {
            var redis = RedisUtils.GetDatabase(ctx);
            string key = $"nhl10:{game.RoutePrefix}:players";

            if (redis != null)
            {
                var cached = await redis.StringGetAsync(key);
                if (cached.HasValue)
                    return Results.Text(cached!, "application/json");
            }

            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var rows = await DbUtils.ReadRows(conn,
                "SELECT DISTINCT gamertag FROM reports WHERE gamertag IS NOT NULL");

            var result = rows.Select(r => r["gamertag"]).ToArray();
            var json = JsonSerializer.Serialize(result);

            if (redis != null)
                await redis.StringSetAsync(key, json, TimeSpan.FromSeconds(30));

            return Results.Text(json, "application/json");
        });

        // PLAYER PROFILE
        app.MapGet($"{prefix}/api/player/{{gamertag}}", async (HttpContext ctx, string gamertag) =>
        {
            var redis = RedisUtils.GetDatabase(ctx);
            string key = $"nhl10:{game.RoutePrefix}:player:{gamertag}";

            if (redis != null)
            {
                var cached = await redis.StringGetAsync(key);
                if (cached.HasValue)
                    return Results.Text(cached!, "application/json");
            }

            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var rows = await DbUtils.ReadRows(conn, """
                                                        SELECT user_id, score
                                                        FROM reports
                                                        WHERE gamertag = @gt
                                                    """, new NpgsqlParameter("gt", gamertag));

            if (rows.Count == 0)
                return Results.NotFound();

            var payload = new
            {
                userId = Convert.ToInt32(rows[0]["user_id"]),
                playerName = gamertag,
                totalGames = rows.Count,
                totalGoals = rows.Sum(r => Convert.ToInt32(r["score"] ?? 0))
            };

            var json = JsonSerializer.Serialize(payload);

            if (redis != null)
                await redis.StringSetAsync(key, json, TimeSpan.FromSeconds(30));

            return Results.Text(json, "application/json");
        });

        // RAW
        app.MapGet($"{prefix}/api/raw/games", async () =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            return Results.Json(await DbUtils.ReadRows(conn, "SELECT * FROM games"));
        });

        app.MapGet($"{prefix}/api/raw/reports", async () =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            return Results.Json(await DbUtils.ReadRows(conn, "SELECT * FROM reports"));
        });

       // GAMES LIST
        app.MapGet($"{prefix}/api/games", async (HttpContext ctx) =>
        {
            var redis = RedisUtils.GetDatabase(ctx);
            string key = $"nhl10:{game.RoutePrefix}:games";

            if (redis != null)
            {
                var cached = await redis.StringGetAsync(key);
                if (cached.HasValue)
                    return Results.Text(cached!, "application/json");
            }

            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var games = await DbUtils.ReadRows(conn,
                "SELECT * FROM games ORDER BY created_at DESC");

            var reports = await DbUtils.ReadRows(conn,
                "SELECT * FROM reports");

            var grouped = reports
                .Where(r => r.ContainsKey("game_id"))
                .GroupBy(r => Convert.ToInt32(r["game_id"]))
                .ToDictionary(g => g.Key, g => g.ToList());

            var result = new List<object>();

            foreach (var g in games)
            {
                int id = Convert.ToInt32(g["game_id"]);
                grouped.TryGetValue(id, out var reps);
                reps ??= new();

                result.Add(new
                {
                    game_id = id,
                    created_at = g["created_at"],
                    fnsh = g.GetValueOrDefault("fnsh"),
                    gtyp = g.GetValueOrDefault("gtyp"),
                    venue = g.GetValueOrDefault("venue"),
                    players = reps.Count,
                    totalGoals = reps.Sum(r => Convert.ToInt32(r["score"] ?? 0)),
                    avgFps = reps.Any() ? reps.Average(r => Convert.ToInt32(r["fpsavg"] ?? 0)) : 0,
                    avgLatency = reps.Any() ? reps.Average(r => Convert.ToInt32(r["lateavgnet"] ?? 0)) : 0,
                    teams = reps.Select(r => new
                    {
                        team_name = r["team_name"],
                        score = r["score"],
                        shots = r["shots"],
                        hits = r["hits"],
                        gamertag = r["gamertag"]
                    }),
                    status = Convert.ToBoolean(g.GetValueOrDefault("fnsh") ?? false)
                        ? "Finished"
                        : "In Progress"
                });
            }

            var json = JsonSerializer.Serialize(result);

            if (redis != null)
                await redis.StringSetAsync(key, json, TimeSpan.FromSeconds(30));

            return Results.Text(json, "application/json");
        });

        // GAME REPORTS
        app.MapGet($"{prefix}/api/game/{{id:int}}/reports", async (int id) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var rows = await DbUtils.ReadRows(conn, """
                SELECT user_id, gamertag, score
                FROM reports
                WHERE game_id = @id
            """, new NpgsqlParameter("id", id));

            return Results.Json(new
            {
                gameId = id,
                reports = rows
            });
        });

        // LEADERBOARD (cached)

        // LEADERBOARD
        app.MapGet($"{prefix}/api/leaderboard/{{range}}", async (HttpContext ctx, string range) =>
        {
            var redis = RedisUtils.GetDatabase(ctx);
            string key = $"nhl10:{game.RoutePrefix}:leaderboard:{range}";

            if (redis != null)
            {
                var cached = await redis.StringGetAsync(key);
                if (cached.HasValue)
                    return Results.Text(cached!, "application/json");
            }

            var from = DbUtils.RangeToDate(range);

            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var cmd = new NpgsqlCommand("""
                                            SELECT r.gamertag,
                                                   SUM(r.score) AS total_goals,
                                                   COUNT(*) AS games_played
                                            FROM reports r
                                            JOIN games g ON g.game_id = r.game_id
                                            WHERE (@from = '0001-01-01'::timestamp OR g.created_at >= @from)
                                            GROUP BY r.gamertag
                                            ORDER BY total_goals DESC
                                        """, conn);

            cmd.Parameters.AddWithValue("from", from);

            var list = new List<object>();
            using var reader = await cmd.ExecuteReaderAsync();

            int rank = 1;
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

            var json = JsonSerializer.Serialize(list);

            if (redis != null)
                await redis.StringSetAsync(key, json, TimeSpan.FromSeconds(60));

            return Results.Text(json, "application/json");
        });

        // GLOBAL STATS
        app.MapGet($"{prefix}/api/stats/global", async (HttpContext ctx) =>
        {
            var redis = RedisUtils.GetDatabase(ctx);
            string key = $"nhl10:{game.RoutePrefix}:stats:global";

            if (redis != null)
            {
                var cached = await redis.StringGetAsync(key);
                if (cached.HasValue)
                    return Results.Text(cached!, "application/json");
            }

            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            int games = Convert.ToInt32(
                await new NpgsqlCommand("SELECT COUNT(*) FROM games", conn).ExecuteScalarAsync());
            int reports = Convert.ToInt32(
                await new NpgsqlCommand("SELECT COUNT(*) FROM reports", conn).ExecuteScalarAsync());
            int players = Convert.ToInt32(
                await new NpgsqlCommand("SELECT COUNT(DISTINCT gamertag) FROM reports", conn).ExecuteScalarAsync());

            var payload = new
            {
                totalGames = games,
                totalReports = reports,
                totalPlayers = players
            };

            var json = JsonSerializer.Serialize(payload);

            if (redis != null)
                await redis.StringSetAsync(key, json, TimeSpan.FromSeconds(60));

            return Results.Text(json, "application/json");
        });

        // LATEST REPORTS
        app.MapGet($"{prefix}/api/reports/latest", async (int? limit) =>
        {
            int max = Math.Clamp(limit ?? 50, 1, 500);

            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            return Results.Json(await DbUtils.ReadRows(conn, $"""
                SELECT *
                FROM reports
                ORDER BY created_at DESC
                LIMIT {max}
            """));
        });

        // USER HISTORY
        app.MapGet($"{prefix}/api/user/{{id:int}}/history", async (int id) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var userRows = await DbUtils.ReadRows(conn,
                "SELECT * FROM reports WHERE user_id=@id ORDER BY created_at DESC",
                new NpgsqlParameter("id", id));

            if (!userRows.Any())
                return Results.Json(Array.Empty<object>());

            var gameIds = userRows.Select(r => Convert.ToInt32(r["game_id"])).ToArray();

            var oppRows = await DbUtils.ReadRows(conn, """
                SELECT * FROM reports
                WHERE game_id = ANY(@ids) AND user_id != @uid
            """,
                new NpgsqlParameter("ids", gameIds),
                new NpgsqlParameter("uid", id));

            foreach (var r in userRows)
            {
                var opp = oppRows.FirstOrDefault(o =>
                    Convert.ToInt32(o["game_id"]) == Convert.ToInt32(r["game_id"]));

                if (opp != null)
                {
                    r["opponent"] = opp["gamertag"];
                    r["opponent_team"] = opp["team_name"];
                    r["opponent_score"] = opp["score"];
                    r["opponent_hits"] = opp["hits"];
                    r["opponent_shots"] = opp["shots"];
                }
            }

            return Results.Json(userRows);
        });

        // GAME SUMMARY
        app.MapGet($"{prefix}/api/games/{{id:int}}/summary", async (int id) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var gameRow = await DbUtils.ReadRows(conn, """
                SELECT 
                    g.game_id,
                    g.created_at,
                    MAX(CASE WHEN r.team = 1 THEN r.team_name END) AS home_team,
                    MAX(CASE WHEN r.team = 0 THEN r.team_name END) AS away_team,
                    COALESCE(SUM(CASE WHEN r.team = 1 THEN r.score END),0) AS home_score,
                    COALESCE(SUM(CASE WHEN r.team = 0 THEN r.score END),0) AS away_score
                FROM games g
                LEFT JOIN reports r ON r.game_id = g.game_id
                WHERE g.game_id = @id
                GROUP BY g.game_id, g.created_at
            """, new NpgsqlParameter("id", id));

            if (gameRow.Count == 0)
                return Results.NotFound();

            var g = gameRow[0];

            var reports = await DbUtils.ReadRows(conn,
                "SELECT user_id, gamertag, score FROM reports WHERE game_id=@id",
                new NpgsqlParameter("id", id));

            string? winner =
                Convert.ToInt32(g["home_score"]) > Convert.ToInt32(g["away_score"])
                    ? g["home_team"]?.ToString()
                    : Convert.ToInt32(g["away_score"]) > Convert.ToInt32(g["home_score"])
                        ? g["away_team"]?.ToString()
                        : null;

            return Results.Json(new
            {
                game = new
                {
                    gameId = g["game_id"],
                    playedAt = g["created_at"],
                    homeTeam = g["home_team"],
                    awayTeam = g["away_team"],
                    homeScore = g["home_score"],
                    awayScore = g["away_score"]
                },
                reports,
                winnerTeam = winner
            });
        });
    }
}
