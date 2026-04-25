/*
 *  NHL11API
 *  TITLES: 11
 */

using APIService.Config;
using APIService.Core;
using Npgsql;

namespace APIService.Games.NHL11;

public static class Nhl11Api
{
    public static void Map(WebApplication app, GameConfig game)
    {
        string prefix = "/" + game.RoutePrefix.Trim('/');

        /*
         * TODO: MISSING:
         * nhl11/status
         */

        //32->64
        static long L(object? v)
            => v == null || v == DBNull.Value ? 0L : Convert.ToInt64(v);

        // 64->32
        static int I(object? v)
            => v == null || v == DBNull.Value ? 0 : Convert.ToInt32(v);

        // GET | Returns players list 
        app.MapGet($"{prefix}/api/players", async () =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var rows = await DbUtils.ReadRows(conn, """
                                                        SELECT DISTINCT gamertag FROM reports
                                                        UNION
                                                        SELECT DISTINCT gamertag FROM so_reports
                                                        UNION
                                                        SELECT DISTINCT gamertag FROM otp_reports
                                                    """);

            return Results.Json(rows.Select(r => r["gamertag"]));
        });

        // GET | Returns player info via gamertag
        app.MapGet($"{prefix}/api/player/{{gamertag}}", async (string gamertag) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var rows = await DbUtils.ReadRows(conn, """
                                                        SELECT user_id, score FROM reports WHERE gamertag=@gt
                                                        UNION ALL
                                                        SELECT user_id, score FROM so_reports WHERE gamertag=@gt
                                                        UNION ALL
                                                        SELECT user_id, score FROM otp_reports WHERE gamertag=@gt
                                                    """, new NpgsqlParameter("gt", gamertag));

            if (rows.Count == 0)
                return Results.NotFound();

            return Results.Json(new
            {
                userId = rows[0]["user_id"],
                playerName = gamertag,
                totalGames = rows.Count,
                totalGoals = rows.Sum(r => I(r["score"]))
            });
        });

        // GET | Return the games lists
        app.MapGet($"{prefix}/api/games", async () =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var games = await DbUtils.ReadRows(conn,
                "SELECT * FROM games ORDER BY created_at DESC");

            var vs = await DbUtils.ReadRows(conn, "SELECT * FROM reports");
            var so = await DbUtils.ReadRows(conn, "SELECT * FROM so_reports");
            var otp = await DbUtils.ReadRows(conn, "SELECT * FROM otp_reports");

            var vsByGame = vs.GroupBy(r => L(r["game_id"])).ToDictionary(g => g.Key, g => g.ToList());
            var soByGame = so.GroupBy(r => L(r["game_id"])).ToDictionary(g => g.Key, g => g.ToList());
            var otpByGame = otp.GroupBy(r => L(r["game_id"])).ToDictionary(g => g.Key, g => g.ToList());

            object BuildGame(Dictionary<string, object?> g, List<Dictionary<string, object?>> reps)
                => new
                {
                    game_id = g["game_id"],
                    created_at = g["created_at"],
                    fnsh = g["fnsh"],
                    gtyp = g["gtyp"],
                    venue = g["venue"],
                    players = reps.Count,
                    totalGoals = reps.Sum(r => I(r["score"])),
                    avgFps = reps.Any() ? reps.Average(r => I(r["fpsavg"])) : 0,
                    avgLatency = reps.Any() ? reps.Average(r => I(r["lateavgnet"])) : 0,
                    teams = reps.Select(r => new
                    {
                        team_name = r["team_name"],
                        score = r["score"],
                        shots = r.GetValueOrDefault("shots"),
                        hits = r.GetValueOrDefault("hits"),
                        gamertag = r["gamertag"]
                    }),
                    status = Convert.ToBoolean(g["fnsh"] ?? false)
                        ? "Finished"
                        : "In Progress"
                };

            return Results.Json(new
            {
                VS = games
                    .Where(g => vsByGame.ContainsKey(L(g["game_id"])))
                    .Select(g => BuildGame(g, vsByGame[L(g["game_id"])])),

                SO = games
                    .Where(g => soByGame.ContainsKey(L(g["game_id"])))
                    .Select(g => BuildGame(g, soByGame[L(g["game_id"])])),

                OTP = games
                    .Where(g => otpByGame.ContainsKey(L(g["game_id"])))
                    .Select(g => BuildGame(g, otpByGame[L(g["game_id"])]))
            });
        });

        // GET | Reports via game id
        app.MapGet($"{prefix}/api/game/{{id:long}}/reports", async (long id) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            return Results.Json(new
            {
                VS = await DbUtils.ReadRows(conn,
                    "SELECT * FROM reports WHERE game_id=@id",
                    new NpgsqlParameter("id", id)
                ),

                SO = await DbUtils.ReadRows(conn,
                    "SELECT * FROM so_reports WHERE game_id=@id",
                    new NpgsqlParameter("id", id)
                ),

                OTP = await DbUtils.ReadRows(conn,
                    "SELECT * FROM otp_reports WHERE game_id=@id",
                    new NpgsqlParameter("id", id)
                )
            });
        });

        // GET | Sumamry of hgame via id
        app.MapGet($"{prefix}/api/games/{{id:long}}/summary", async (long id) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var rows = await DbUtils.ReadRows(conn, """
                                                    SELECT game_id, user_id, home, score
                                                    FROM reports WHERE game_id=@id
                                                    UNION ALL
                                                    SELECT game_id, user_id, home, score
                                                    FROM so_reports WHERE game_id=@id
                                                    UNION ALL
                                                    SELECT game_id, user_id, home, score
                                                    FROM otp_reports WHERE game_id=@id
                                                    """, new NpgsqlParameter("id", id));

            if (!rows.Any())
                return Results.NotFound();

            var home = rows.Where(r => Convert.ToInt32(r["home"]) == 1);
            var away = rows.Where(r => Convert.ToInt32(r["home"]) == 0);

            return Results.Json(new
            {
                gameId = id,
                homeScore = home.Sum(r => Convert.ToInt32(r["score"] ?? 0)),
                awayScore = away.Sum(r => Convert.ToInt32(r["score"] ?? 0))
            });
        });

        // GET | Latest reports via limit
        app.MapGet($"{prefix}/api/reports/latest", async (int? limit) =>
        {
            int max = Math.Clamp(limit ?? 50, 1, 500);

            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            return Results.Json(await DbUtils.ReadRows(conn, $"""
                                                                  SELECT * FROM (
                                                                      SELECT * FROM reports
                                                                      UNION ALL
                                                                      SELECT * FROM so_reports
                                                                      UNION ALL
                                                                      SELECT * FROM otp_reports
                                                                  ) x
                                                                  ORDER BY created_at DESC
                                                                  LIMIT {max}
                                                              """));
        });

        // GET | Users game history
        app.MapGet($"{prefix}/api/user/{{id:long}}/history", async (long id) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var userRows = await DbUtils.ReadRows(conn, """
                                                            SELECT game_id, user_id, gamertag, team_name, score, created_at
                                                            FROM reports WHERE user_id=@id

                                                            UNION ALL

                                                            SELECT game_id, user_id, gamertag, team_name, score, created_at
                                                            FROM so_reports WHERE user_id=@id

                                                            UNION ALL

                                                            SELECT game_id, user_id, gamertag, team_name, score, created_at
                                                            FROM otp_reports WHERE user_id=@id
                                                        """, new NpgsqlParameter("id", id));

            if (!userRows.Any())
                return Results.Json(Array.Empty<object>());

            var gameIds = userRows
                .Select(r => Convert.ToInt64(r["game_id"]))
                .Distinct()
                .ToArray();

            var allRows = await DbUtils.ReadRows(conn, """
                                                           SELECT game_id, user_id, gamertag, team_name, score
                                                           FROM reports WHERE game_id = ANY(@ids)

                                                           UNION ALL

                                                           SELECT game_id, user_id, gamertag, team_name, score
                                                           FROM so_reports WHERE game_id = ANY(@ids)

                                                           UNION ALL

                                                           SELECT game_id, user_id, gamertag, team_name, score
                                                           FROM otp_reports WHERE game_id = ANY(@ids)
                                                       """, new NpgsqlParameter("ids", gameIds));

            foreach (var r in userRows)
            {
                var opp = allRows.FirstOrDefault(o =>
                    Convert.ToInt64(o["game_id"]) == Convert.ToInt64(r["game_id"]) &&
                    Convert.ToInt64(o["user_id"]) != Convert.ToInt64(r["user_id"])
                );

                if (opp != null)
                {
                    r["opponent"] = opp["gamertag"];
                    r["opponent_team"] = opp["team_name"];
                    r["opponent_score"] = opp["score"];
                }
            }

            return Results.Json(userRows);
        });

        // GET | Leaderboard with range selection
        app.MapGet($"{prefix}/api/leaderboard/{{range}}", async (string range) =>
        {
            var from = DbUtils.RangeToDate(range);

            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var sql = """
                          SELECT gamertag, SUM(score) AS total_goals, COUNT(*) AS games_played
                          FROM (
                              SELECT gamertag, score, created_at FROM reports
                              UNION ALL
                              SELECT gamertag, score, created_at FROM so_reports
                              UNION ALL
                              SELECT gamertag, score, created_at FROM otp_reports
                          ) x
                          WHERE (@from = '0001-01-01'::timestamp OR created_at >= @from)
                          GROUP BY gamertag
                          ORDER BY total_goals DESC
                      """;

            using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("from", from);

            var list = new List<object>();
            int rank = 1;

            using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
            {
                list.Add(new
                {
                    gamertag = r["gamertag"],
                    totalGoals = r["total_goals"],
                    gamesPlayed = r["games_played"],
                    rank = rank++
                });
            }

            return Results.Json(list);
        });

        // GET | Global stats
        app.MapGet($"{prefix}/api/stats/global", async () =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var games = Convert.ToInt32(
                await new NpgsqlCommand("SELECT COUNT(*) FROM games", conn)
                    .ExecuteScalarAsync()
            );

            var reports = Convert.ToInt32(
                await new NpgsqlCommand("""
                                            SELECT COUNT(*) FROM (
                                                SELECT game_id FROM reports
                                                UNION ALL
                                                SELECT game_id FROM so_reports
                                                UNION ALL
                                                SELECT game_id FROM otp_reports
                                            ) x
                                        """, conn).ExecuteScalarAsync()
            );

            var players = Convert.ToInt32(
                await new NpgsqlCommand("""
                                            SELECT COUNT(DISTINCT gamertag) FROM (
                                                SELECT gamertag FROM reports
                                                UNION
                                                SELECT gamertag FROM so_reports
                                                UNION
                                                SELECT gamertag FROM otp_reports
                                            ) x
                                        """, conn).ExecuteScalarAsync()
            );

            return Results.Json(new
            {
                totalGames = games,
                totalReports = reports,
                totalPlayers = players
            });
        });


        // GET | Returns raw games from games table
        app.MapGet($"{prefix}/api/raw/games", async () =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            return Results.Json(
                await DbUtils.ReadRows(conn, "SELECT * FROM games")
            );
        });

        // GET | Returns raw reports from reports table
        app.MapGet($"{prefix}/api/raw/reports", async () =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            return Results.Json(
                await DbUtils.ReadRows(conn, "SELECT * FROM reports")
            );
        });
    }
}