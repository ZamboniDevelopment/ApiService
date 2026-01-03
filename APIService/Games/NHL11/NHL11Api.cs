using APIService.Config;
using APIService.Core;

using Npgsql;

namespace APIService.Games.NHL11;

public static class NHL11Api
{
    public static void Map(WebApplication app, GameConfig game)
    {
        string prefix = "/" + game.RoutePrefix.Trim('/');
        
        /*
         * TODO: MISSING:
         * nhl11/status
         * + bunch of other endpoints
         * + redis support
         */
        
        // GET | Returns players list (TODO: Redis) 
        app.MapGet($"{prefix}/api/players", async () =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var rows = await DbUtils.ReadRows(conn,
                "SELECT DISTINCT gamertag FROM reports");

            return Results.Json(rows.Select(r => r["gamertag"]));
        });

        // GET | Returns player info via gamertag (TODO: Redis)
        app.MapGet($"{prefix}/api/player/{{gamertag}}", async (string gamertag) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var rows = await DbUtils.ReadRows(conn, """
                SELECT user_id, score
                FROM reports
                WHERE gamertag = @gt
            """, new NpgsqlParameter("gt", gamertag));

            if (rows.Count == 0)
                return Results.NotFound();

            var userId = Convert.ToInt32(rows[0]["user_id"]);
            var totalGames = rows.Count;
            var totalGoals = rows.Sum(r => Convert.ToInt32(r["score"] ?? 0));

            return Results.Json(new
            {
                userId,
                playerName = gamertag,
                totalGames,
                totalGoals
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