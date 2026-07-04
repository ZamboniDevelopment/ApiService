/*
 *  HUTAPI
 *  TITLES: 12
 */

using APIService.Config;
using APIService.Core;
using Npgsql;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace APIService.Games.HUT;

public static class HutApi
{
    private static readonly Regex IdentRegex =
        new("^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled);
    private static string Ident(string name)
    {
        if (string.IsNullOrWhiteSpace(name) || !IdentRegex.IsMatch(name))
            throw new InvalidOperationException($"Invalid catalog table name: '{name}'");
        return name;
    }
    private static string CardState(object? v) => Convert.ToInt32(v ?? 0) switch
    {
        1 => "Free",
        2 => "InGame",
        3 => "InCardSell",
        4 => "OnlineSwap",
        5 => "ImprovedSale",
        6 => "PartOfOffer",
        100 => "ActiveBadge",
        101 => "ActiveHomeKit",
        102 => "ActiveAwayKit",
        104 => "ActiveStadium",
        255 => "SearchActive",
        _ => "Invalid"
    };
    private static string DeckType(object? v) => Convert.ToInt32(v ?? 0) switch
    {
        1 => "Squad",
        2 => "Training",
        3 => "Gameplay",
        4 => "Active",
        5 => "Escrow",
        6 => "Unassigned",
        7 => "Stickerbook",
        8 => "Inbox",
        9 => "General",
        _ => "Invalid"
    };
    private static string TradeState(object? v) => Convert.ToInt32(v ?? 0) switch
    {
        1 => "Active",
        2 => "Canceled",
        3 => "Expired",
        4 => "Closed",
        _ => "Invalid"
    };
    private static string OfferState(object? v) => Convert.ToInt32(v ?? 0) switch
    {
        1 => "Active",
        2 => "Accepted",
        3 => "Rejected",
        4 => "Canceled",
        5 => "Outbid",
        6 => "TradeClosed",
        7 => "WinningBid",
        _ => "Invalid"
    };
    private static string Position(object? v) => Convert.ToInt32(v ?? -1) switch
    {
        0 => "C",
        1 => "LW",
        2 => "RW",
        3 => "LD",
        4 => "RD",
        5 => "GK",
        _ => "Unknown"
    };
    private static long L(object? v)
        => v == null || v == DBNull.Value ? 0L : Convert.ToInt64(v);
    private static int ArrAt(object? arrObj, int i)
        => arrObj is int[] a && a.Length > i ? a[i] : 0;
    private static object Record(object? statsObj)
    {
        int w = ArrAt(statsObj, 8);
        int l = ArrAt(statsObj, 9);
        int o = ArrAt(statsObj, 10);
        int gp = w + l + o;
        return new
        {
            wins = w,
            losses = l,
            otl = o,
            gamesPlayed = gp,
            points = 2 * w + o,
            winPct = gp > 0 ? Math.Round((double)w / gp, 4) : 0
        };
    }
    private static int GamesPlayed(object? listStatsObj) => ArrAt(listStatsObj, 0);
    private static string SortCol(string? sort, Dictionary<string, string> allowed, string fallback)
        => sort != null && allowed.TryGetValue(sort.ToLowerInvariant(), out var col) ? col : fallback;

    public static void Map(WebApplication app, GameConfig game)
    {
        string prefix = "/" + game.RoutePrefix.Trim('/');
        var cat = game.HutCatalog ?? new HutCatalogConfig();
        string tPlayers = Ident(cat.PlayerCards);
        string tKits = Ident(cat.KitCards);
        string tBadges = Ident(cat.Badges);
        string tTraining = Ident(cat.TrainingCards);
        string tContracts = Ident(cat.ContractCards);
        string tHealing = Ident(cat.HealingCards);
        string tCoaches = Ident(cat.HeadCoachCards);
        string tLeagues = Ident(cat.Leagues);
        string tStadiums = Ident(cat.Stadiums);

        app.MapGet($"{prefix}/api/hut/status", async () =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            async Task<long> Count(string sql)
                => Convert.ToInt64(await new NpgsqlCommand(sql, conn).ExecuteScalarAsync() ?? 0L);

            return Results.Json(new
            {
                managers = await Count("SELECT COUNT(*) FROM hut_gamer_info"),
                cards = await Count("SELECT COUNT(*) FROM hut_cards"),
                squads = await Count("SELECT COUNT(*) FROM hut_squad_info"),
                activeSquads = await Count("SELECT COUNT(*) FROM hut_squad_info WHERE active = true"),
                openTrades = await Count("SELECT COUNT(*) FROM hut_trade_info WHERE trade_state = 1"),
                offers = await Count("SELECT COUNT(*) FROM hut_offer_info"),
                tournaments = await Count("SELECT COUNT(*) FROM hut_tournaments"),
                catalogPlayers = await Count($"SELECT COUNT(*) FROM {tPlayers}")
            });
        });

        app.MapGet($"{prefix}/api/hut/managers", async () =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var rows = await DbUtils.ReadRows(conn, """
                SELECT g.user_id,
                       g.team_name,
                       g.team_abbreviation,
                       g.logo_url,
                       gi.pucks
                FROM hut_gamer_info g
                LEFT JOIN hut_general_info gi ON gi.user_id = g.user_id
                ORDER BY g.user_id
            """);

            return Results.Json(rows);
        });

        app.MapGet($"{prefix}/api/hut/manager/{{userId:long}}", async (long userId) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var gamer = await DbUtils.ReadRows(conn,
                "SELECT * FROM hut_gamer_info WHERE user_id=@id",
                new NpgsqlParameter("id", userId));

            if (gamer.Count == 0)
                return Results.NotFound();

            var general = await DbUtils.ReadRows(conn,
                "SELECT user_id, pucks, stats FROM hut_general_info WHERE user_id=@id",
                new NpgsqlParameter("id", userId));

            var version = await DbUtils.ReadRows(conn,
                "SELECT * FROM hut_version_info WHERE user_id=@id",
                new NpgsqlParameter("id", userId));

            var cardCount = Convert.ToInt64(await new NpgsqlCommand(
                "SELECT COUNT(*) FROM hut_cards WHERE user_id=@id", conn)
                { Parameters = { new NpgsqlParameter("id", userId) } }
                .ExecuteScalarAsync() ?? 0L);

            var squads = await DbUtils.ReadRows(conn, """
                SELECT squad_id, squad_name, star_rating, rating_off, rating_def,
                       rating_gk, chemistry, active
                FROM hut_squad_info WHERE user_id=@id ORDER BY squad_id
            """, new NpgsqlParameter("id", userId));

            return Results.Json(new
            {
                gamerInfo = gamer[0],
                general = general.Count > 0 ? general[0] : null,
                record = general.Count > 0 ? Record(general[0]["stats"]) : Record(null),
                versions = version.Count > 0 ? version[0] : null,
                cardCount,
                squads
            });
        });

        app.MapGet($"{prefix}/api/hut/manager/{{userId:long}}/cards",
            async (long userId, int? deck, int? state, int? limit, int? offset) =>
        {
            int max = Math.Clamp(limit ?? 100, 1, 1000);
            int skip = Math.Max(offset ?? 0, 0);

            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var sql = new StringBuilder($"""
                SELECT c.card_id, c.db_id, c.deck_type, c.state_id, c.sub_type,
                       c.rating, c.rare_flag, c.preferred_position_id, c.salary_cap,
                       c.team_id, c.uses_remaining, c.attributes, c.date_issued,
                       c.list_stats, c.career_remaining, c.injury_type, c.injury_games,
                       p.firstname, p.lastname, p.commonname, p.nation
                FROM hut_cards c
                LEFT JOIN {tPlayers} p ON p.carddbid = c.db_id
                WHERE c.user_id=@id
            """);

            var ps = new List<NpgsqlParameter> { new("id", userId) };
            if (deck.HasValue) { sql.Append(" AND c.deck_type=@deck"); ps.Add(new("deck", deck.Value)); }
            if (state.HasValue) { sql.Append(" AND c.state_id=@state"); ps.Add(new("state", state.Value)); }
            sql.Append($" ORDER BY c.card_id LIMIT {max} OFFSET {skip}");

            var rows = await DbUtils.ReadRows(conn, sql.ToString(), ps.ToArray());

            return Results.Json(rows.Select(r => new
            {
                cardId = r["card_id"],
                dbId = r["db_id"],
                name = r["commonname"] ?? $"{r["firstname"]} {r["lastname"]}".Trim(),
                rating = r["rating"],
                position = Position(r["preferred_position_id"]),
                subType = r["sub_type"],
                deck = DeckType(r["deck_type"]),
                deckId = r["deck_type"],
                state = CardState(r["state_id"]),
                stateId = r["state_id"],
                rare = r["rare_flag"],
                salaryCap = r["salary_cap"],
                teamId = r["team_id"],
                nation = r["nation"],
                usesRemaining = r["uses_remaining"],
                attributes = r["attributes"],
                gamesPlayed = GamesPlayed(r["list_stats"]),
                careerRemaining = r["career_remaining"],
                retired = Convert.ToInt32(r["career_remaining"] ?? 0) - GamesPlayed(r["list_stats"]) <= 0,
                injuryType = r["injury_type"],
                injuryGames = r["injury_games"],
                listStats = r["list_stats"],
                dateIssued = r["date_issued"]
            }));
        });

        app.MapGet($"{prefix}/api/hut/manager/{{userId:long}}/squads",
            async (long userId, bool? active) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var sql = "SELECT * FROM hut_squad_info WHERE user_id=@id";
            var ps = new List<NpgsqlParameter> { new("id", userId) };
            if (active.HasValue) { sql += " AND active=@active"; ps.Add(new("active", active.Value)); }
            sql += " ORDER BY squad_id";

            return Results.Json(await DbUtils.ReadRows(conn, sql, ps.ToArray()));
        });

        app.MapGet($"{prefix}/api/hut/manager/{{userId:long}}/tournaments", async (long userId) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            return Results.Json(await DbUtils.ReadRows(conn, """
                SELECT user_id, tournament_type, tournament_id, blaze_tournament_id, active
                FROM hut_tournaments WHERE user_id=@id ORDER BY tournament_type
            """, new NpgsqlParameter("id", userId)));
        });

        app.MapGet($"{prefix}/api/hut/manager/{{userId:long}}/offers", async (long userId) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var rows = await DbUtils.ReadRows(conn, """
                SELECT offer_id, trade_id, offer_state, credits, card_ids, created_at_seconds
                FROM hut_offer_info WHERE user_id=@id ORDER BY offer_id DESC
            """, new NpgsqlParameter("id", userId));

            return Results.Json(rows.Select(r => new
            {
                offerId = r["offer_id"],
                tradeId = r["trade_id"],
                state = OfferState(r["offer_state"]),
                stateId = r["offer_state"],
                credits = r["credits"],
                cardIds = r["card_ids"],
                createdAtSeconds = r["created_at_seconds"]
            }));
        });

        app.MapGet($"{prefix}/api/hut/manager/{{userId:long}}/watching", async (long userId) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            return Results.Json(await DbUtils.ReadRows(conn, """
                SELECT w.trade_id, t.card_id, t.starting_price, t.highest_bid,
                       t.buy_out_price, t.trade_state, t.seller_name
                FROM hut_watching w
                LEFT JOIN hut_trade_info t ON t.trade_id = w.trade_id
                WHERE w.user_id=@id
            """, new NpgsqlParameter("id", userId)));
        });

        app.MapGet($"{prefix}/api/hut/card/{{cardId:long}}", async (long cardId) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var rows = await DbUtils.ReadRows(conn, $"""
                SELECT c.*, p.firstname, p.lastname, p.commonname, p.nation
                FROM hut_cards c
                LEFT JOIN {tPlayers} p ON p.carddbid = c.db_id
                WHERE c.card_id=@id
            """, new NpgsqlParameter("id", cardId));

            if (rows.Count == 0)
                return Results.NotFound();

            var r = rows[0];
            r["state"] = CardState(r["state_id"]);
            r["deck"] = DeckType(r["deck_type"]);
            r["position"] = Position(r["preferred_position_id"]);
            r["games_played"] = GamesPlayed(r["list_stats"]);
            r["retired"] = Convert.ToInt32(r["career_remaining"] ?? 0) - GamesPlayed(r["list_stats"]) <= 0;
            return Results.Json(r);
        });
        
        app.MapGet($"{prefix}/api/hut/market/trades",
            async (int? state, int? limit, int? offset) =>
        {
            int max = Math.Clamp(limit ?? 50, 1, 500);
            int skip = Math.Max(offset ?? 0, 0);
            int wantState = state ?? 1;

            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var rows = await DbUtils.ReadRows(conn, $"""
                SELECT t.trade_id, t.user_id, t.card_id, t.starting_price,
                       t.highest_bid, t.buy_out_price, t.seller_name, t.trade_state,
                       t.duration_seconds, t.created_at_seconds,
                       c.db_id, c.rating, c.preferred_position_id,
                       p.firstname, p.lastname, p.commonname
                FROM hut_trade_info t
                LEFT JOIN hut_cards c ON c.card_id = t.card_id
                LEFT JOIN {tPlayers} p ON p.carddbid = c.db_id
                WHERE t.trade_state=@state
                ORDER BY t.trade_id DESC
                LIMIT {max} OFFSET {skip}
            """, new NpgsqlParameter("state", wantState));

            return Results.Json(rows.Select(r => new
            {
                tradeId = r["trade_id"],
                sellerId = r["user_id"],
                sellerName = r["seller_name"],
                cardId = r["card_id"],
                player = r["commonname"] ?? $"{r["firstname"]} {r["lastname"]}".Trim(),
                rating = r["rating"],
                position = Position(r["preferred_position_id"]),
                startingPrice = r["starting_price"],
                highestBid = r["highest_bid"],
                buyOutPrice = r["buy_out_price"],
                state = TradeState(r["trade_state"]),
                durationSeconds = r["duration_seconds"],
                createdAtSeconds = r["created_at_seconds"]
            }));
        });

        app.MapGet($"{prefix}/api/hut/market/trade/{{tradeId:long}}", async (long tradeId) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var trade = await DbUtils.ReadRows(conn, $"""
                SELECT t.*, c.db_id, c.rating, c.preferred_position_id,
                       p.firstname, p.lastname, p.commonname
                FROM hut_trade_info t
                LEFT JOIN hut_cards c ON c.card_id = t.card_id
                LEFT JOIN {tPlayers} p ON p.carddbid = c.db_id
                WHERE t.trade_id=@id
            """, new NpgsqlParameter("id", tradeId));

            if (trade.Count == 0)
                return Results.NotFound();

            var t = trade[0];
            t["trade_state_name"] = TradeState(t["trade_state"]);

            var offers = await DbUtils.ReadRows(conn, """
                SELECT offer_id, user_id, offer_state, credits, card_ids, created_at_seconds
                FROM hut_offer_info WHERE trade_id=@id ORDER BY credits DESC
            """, new NpgsqlParameter("id", tradeId));

            var watchers = Convert.ToInt64(await new NpgsqlCommand(
                "SELECT COUNT(*) FROM hut_watching WHERE trade_id=@id", conn)
                { Parameters = { new NpgsqlParameter("id", tradeId) } }
                .ExecuteScalarAsync() ?? 0L);

            return Results.Json(new
            {
                trade = t,
                offers = offers.Select(o => new
                {
                    offerId = o["offer_id"],
                    bidderId = o["user_id"],
                    state = OfferState(o["offer_state"]),
                    credits = o["credits"],
                    cardIds = o["card_ids"],
                    createdAtSeconds = o["created_at_seconds"]
                }),
                watchers
            });
        });
        
        app.MapGet($"{prefix}/api/hut/leaderboard/pucks", async (int? limit) =>
        {
            int max = Math.Clamp(limit ?? 50, 1, 500);

            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var rows = await DbUtils.ReadRows(conn, $"""
                SELECT gi.user_id, gi.pucks, g.team_name, g.team_abbreviation
                FROM hut_general_info gi
                LEFT JOIN hut_gamer_info g ON g.user_id = gi.user_id
                WHERE gi.pucks IS NOT NULL
                ORDER BY gi.pucks DESC
                LIMIT {max}
            """);

            int rank = 1;
            return Results.Json(rows.Select(r => new
            {
                rank = rank++,
                userId = r["user_id"],
                teamName = r["team_name"],
                teamAbbreviation = r["team_abbreviation"],
                pucks = r["pucks"]
            }));
        });
        
        app.MapGet($"{prefix}/api/hut/catalog/players",
            async (HttpContext ctx, string? name, int? team, int? position,
                   int? minRating, int? rare, int? limit, int? offset) =>
        {
            int max = Math.Clamp(limit ?? 50, 1, 200);
            int skip = Math.Max(offset ?? 0, 0);

            bool cacheable = name == null && team == null && position == null
                             && minRating == null && rare == null && skip == 0;
            var redis = cacheable ? RedisUtils.GetDatabase(ctx) : null;
            string key = $"hut:{game.RoutePrefix}:players:{max}";

            if (redis != null)
            {
                var cached = await redis.StringGetAsync(key);
                if (cached.HasValue)
                    return Results.Text(cached!, "application/json");
            }

            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var sql = new StringBuilder($"""
                SELECT carddbid, firstname, lastname, commonname, rating,
                       fieldpos, preferredposition, nation, teamid, rare
                FROM {tPlayers} WHERE 1=1
            """);
            var ps = new List<NpgsqlParameter>();

            if (!string.IsNullOrWhiteSpace(name))
            {
                sql.Append(" AND (firstname ILIKE @n OR lastname ILIKE @n OR commonname ILIKE @n)");
                ps.Add(new("n", $"%{name}%"));
            }
            if (team.HasValue) { sql.Append(" AND teamid=@team"); ps.Add(new("team", team.Value)); }
            if (position.HasValue) { sql.Append(" AND fieldpos=@pos"); ps.Add(new("pos", position.Value)); }
            if (minRating.HasValue) { sql.Append(" AND rating>=@mr"); ps.Add(new("mr", minRating.Value)); }
            if (rare.HasValue) { sql.Append(" AND rare=@rare"); ps.Add(new("rare", rare.Value)); }

            sql.Append($" ORDER BY rating DESC, lastname ASC LIMIT {max} OFFSET {skip}");

            var rows = await DbUtils.ReadRows(conn, sql.ToString(), ps.ToArray());

            var result = rows.Select(r => new
            {
                dbId = r["carddbid"],
                name = r["commonname"] ?? $"{r["firstname"]} {r["lastname"]}".Trim(),
                firstName = r["firstname"],
                lastName = r["lastname"],
                rating = r["rating"],
                position = Position(r["fieldpos"]),
                preferredPosition = r["preferredposition"],
                nation = r["nation"],
                teamId = r["teamid"],
                rare = r["rare"]
            });

            var json = JsonSerializer.Serialize(result);
            if (redis != null)
                await redis.StringSetAsync(key, json, TimeSpan.FromSeconds(60));

            return Results.Text(json, "application/json");
        });

        app.MapGet($"{prefix}/api/hut/catalog/player/{{dbId:long}}", async (long dbId) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var rows = await DbUtils.ReadRows(conn, $"""
                SELECT p.*, l.leagueid
                FROM {tPlayers} p
                LEFT JOIN {tLeagues} l ON l.teamid = p.teamid
                WHERE p.carddbid=@id LIMIT 1
            """, new NpgsqlParameter("id", (int)dbId));

            if (rows.Count == 0)
                return Results.NotFound();

            var r = rows[0];
            r["position"] = Position(r["fieldpos"]);
            return Results.Json(r);
        });
        
        void MapCatalogList(string route, string table, string orderBy = "carddbid")
        {
            app.MapGet($"{prefix}/api/hut/catalog/{route}", async (int? limit, int? offset) =>
            {
                int max = Math.Clamp(limit ?? 200, 1, 2000);
                int skip = Math.Max(offset ?? 0, 0);

                await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
                await conn.OpenAsync();

                return Results.Json(await DbUtils.ReadRows(conn,
                    $"SELECT * FROM {Ident(table)} ORDER BY {Ident(orderBy)} LIMIT {max} OFFSET {skip}"));
            });
        }

        MapCatalogList("kits", tKits);
        MapCatalogList("badges", tBadges);
        MapCatalogList("training", tTraining);
        MapCatalogList("contracts", tContracts);
        MapCatalogList("healing", tHealing);
        MapCatalogList("headcoaches", tCoaches);
        MapCatalogList("stadiums", tStadiums);

        app.MapGet($"{prefix}/api/hut/catalog/leagues", async () =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();
            return Results.Json(await DbUtils.ReadRows(conn,
                $"SELECT teamid, leagueid FROM {tLeagues} ORDER BY leagueid, teamid"));
        });
        
        app.MapGet($"{prefix}/api/hut/stats/managers",
            async (string? sort, int? minGames, int? limit) =>
        {
            int max = Math.Clamp(limit ?? 50, 1, 500);
            int floor = Math.Max(minGames ?? 0, 0);

            string col = SortCol(sort, new()
            {
                ["wins"] = "wins", ["losses"] = "losses", ["otl"] = "otl",
                ["points"] = "points", ["winpct"] = "win_pct",
                ["games"] = "games_played", ["pucks"] = "pucks"
            }, "wins");

            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var rows = await DbUtils.ReadRows(conn, $"""
                SELECT g.user_id,
                       gm.team_name,
                       gm.team_abbreviation,
                       g.pucks,
                       COALESCE(g.stats[9], 0)  AS wins,
                       COALESCE(g.stats[10], 0) AS losses,
                       COALESCE(g.stats[11], 0) AS otl,
                       (COALESCE(g.stats[9],0) + COALESCE(g.stats[10],0) + COALESCE(g.stats[11],0)) AS games_played,
                       (2 * COALESCE(g.stats[9],0) + COALESCE(g.stats[11],0)) AS points,
                       CASE WHEN (COALESCE(g.stats[9],0) + COALESCE(g.stats[10],0) + COALESCE(g.stats[11],0)) > 0
                            THEN ROUND(COALESCE(g.stats[9],0)::numeric
                                 / (COALESCE(g.stats[9],0) + COALESCE(g.stats[10],0) + COALESCE(g.stats[11],0)), 4)
                            ELSE 0 END AS win_pct
                FROM hut_general_info g
                LEFT JOIN hut_gamer_info gm ON gm.user_id = g.user_id
                WHERE (COALESCE(g.stats[9],0) + COALESCE(g.stats[10],0) + COALESCE(g.stats[11],0)) >= @floor
                ORDER BY {col} DESC NULLS LAST
                LIMIT {max}
            """, new NpgsqlParameter("floor", floor));

            int rank = 1;
            return Results.Json(rows.Select(r => new
            {
                rank = rank++,
                userId = r["user_id"],
                teamName = r["team_name"],
                teamAbbreviation = r["team_abbreviation"],
                wins = r["wins"],
                losses = r["losses"],
                otl = r["otl"],
                gamesPlayed = r["games_played"],
                points = r["points"],
                winPct = r["win_pct"],
                pucks = r["pucks"]
            }));
        });

        app.MapGet($"{prefix}/api/hut/stats/cards/most-owned",
            async (int? position, int? minRating, int? limit) =>
        {
            int max = Math.Clamp(limit ?? 50, 1, 500);

            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var sql = new StringBuilder($"""
                SELECT c.db_id,
                       COUNT(*)                 AS copies,
                       COUNT(DISTINCT c.user_id) AS owners,
                       p.firstname, p.lastname, p.commonname, p.rating, p.fieldpos, p.rare
                FROM hut_cards c
                JOIN {tPlayers} p ON p.carddbid = c.db_id
                WHERE 1=1
            """);
            var ps = new List<NpgsqlParameter>();
            if (position.HasValue) { sql.Append(" AND p.fieldpos=@pos"); ps.Add(new("pos", position.Value)); }
            if (minRating.HasValue) { sql.Append(" AND p.rating>=@mr"); ps.Add(new("mr", minRating.Value)); }
            sql.Append($"""
                 GROUP BY c.db_id, p.firstname, p.lastname, p.commonname, p.rating, p.fieldpos, p.rare
                 ORDER BY copies DESC
                 LIMIT {max}
            """);

            var rows = await DbUtils.ReadRows(conn, sql.ToString(), ps.ToArray());

            return Results.Json(rows.Select(r => new
            {
                dbId = r["db_id"],
                name = r["commonname"] ?? $"{r["firstname"]} {r["lastname"]}".Trim(),
                rating = r["rating"],
                position = Position(r["fieldpos"]),
                rare = r["rare"],
                copies = r["copies"],
                owners = r["owners"]
            }));
        });

        app.MapGet($"{prefix}/api/hut/stats/cards/most-played", async (int? limit) =>
        {
            int max = Math.Clamp(limit ?? 50, 1, 500);

            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var rows = await DbUtils.ReadRows(conn, $"""
                SELECT c.db_id,
                       SUM(COALESCE(c.list_stats[1], 0)) AS total_games,
                       COUNT(*)                          AS copies,
                       p.firstname, p.lastname, p.commonname, p.rating, p.fieldpos
                FROM hut_cards c
                JOIN {tPlayers} p ON p.carddbid = c.db_id
                GROUP BY c.db_id, p.firstname, p.lastname, p.commonname, p.rating, p.fieldpos
                HAVING SUM(COALESCE(c.list_stats[1], 0)) > 0
                ORDER BY total_games DESC
                LIMIT {max}
            """);

            return Results.Json(rows.Select(r => new
            {
                dbId = r["db_id"],
                name = r["commonname"] ?? $"{r["firstname"]} {r["lastname"]}".Trim(),
                rating = r["rating"],
                position = Position(r["fieldpos"]),
                totalGamesPlayed = r["total_games"],
                copies = r["copies"]
            }));
        });

        app.MapGet($"{prefix}/api/hut/stats/card/{{dbId:long}}/usage", async (long dbId) =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var agg = await DbUtils.ReadRows(conn, """
                SELECT
                    COUNT(*)                                   AS copies,
                    COUNT(DISTINCT user_id)                    AS owners,
                    COALESCE(SUM(COALESCE(list_stats[1],0)),0) AS total_games,
                    COALESCE(ROUND(AVG(NULLIF(uses_remaining,0)),2),0) AS avg_uses_remaining,
                    COUNT(*) FILTER (WHERE deck_type = 1)      AS in_squad_deck,
                    COUNT(*) FILTER (WHERE state_id = 3)       AS listed_for_sale
                FROM hut_cards WHERE db_id = @id
            """, new NpgsqlParameter("id", (int)dbId));

            var card = await DbUtils.ReadRows(conn,
                $"SELECT carddbid, firstname, lastname, commonname, rating, fieldpos, rare, teamid " +
                $"FROM {tPlayers} WHERE carddbid=@id LIMIT 1",
                new NpgsqlParameter("id", (int)dbId));

            var a = agg[0];
            return Results.Json(new
            {
                dbId,
                card = card.Count > 0 ? new
                {
                    name = card[0]["commonname"] ?? $"{card[0]["firstname"]} {card[0]["lastname"]}".Trim(),
                    rating = card[0]["rating"],
                    position = Position(card[0]["fieldpos"]),
                    rare = card[0]["rare"],
                    teamId = card[0]["teamid"]
                } : null,
                copies = a["copies"],
                owners = a["owners"],
                totalGamesPlayed = a["total_games"],
                avgUsesRemaining = a["avg_uses_remaining"],
                inSquadDeck = a["in_squad_deck"],
                listedForSale = a["listed_for_sale"]
            });
        });

        app.MapGet($"{prefix}/api/hut/stats/squads/top",
            async (string? sort, bool? active, int? limit) =>
        {
            int max = Math.Clamp(limit ?? 50, 1, 500);
            string col = SortCol(sort, new()
            {
                ["star"] = "star_rating", ["off"] = "rating_off", ["def"] = "rating_def",
                ["gk"] = "rating_gk", ["chemistry"] = "chemistry"
            }, "star_rating");

            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var sql = new StringBuilder($"""
                SELECT s.squad_id, s.user_id, s.squad_name, s.star_rating,
                       s.rating_off, s.rating_def, s.rating_gk, s.chemistry,
                       s.formation_id, s.active, gm.team_name
                FROM hut_squad_info s
                LEFT JOIN hut_gamer_info gm ON gm.user_id = s.user_id
                WHERE 1=1
            """);
            var ps = new List<NpgsqlParameter>();
            if (active.HasValue) { sql.Append(" AND s.active=@active"); ps.Add(new("active", active.Value)); }
            sql.Append($" ORDER BY {col} DESC NULLS LAST LIMIT {max}");

            return Results.Json(await DbUtils.ReadRows(conn, sql.ToString(), ps.ToArray()));
        });

        app.MapGet($"{prefix}/api/hut/stats/squads/formations", async () =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            return Results.Json(await DbUtils.ReadRows(conn, """
                SELECT formation_id,
                       COUNT(*) AS squads,
                       COALESCE(ROUND(AVG(star_rating), 2), 0) AS avg_star_rating,
                       COALESCE(ROUND(AVG(chemistry), 2), 0)   AS avg_chemistry
                FROM hut_squad_info
                GROUP BY formation_id
                ORDER BY squads DESC
            """));
        });

        app.MapGet($"{prefix}/api/hut/stats/economy", async () =>
        {
            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            var pucks = (await DbUtils.ReadRows(conn, """
                SELECT COUNT(*) AS managers,
                       COALESCE(SUM(pucks), 0) AS total_pucks,
                       COALESCE(ROUND(AVG(pucks)), 0) AS avg_pucks,
                       COALESCE(MAX(pucks), 0) AS max_pucks
                FROM hut_general_info
            """))[0];

            var market = (await DbUtils.ReadRows(conn, """
                SELECT COUNT(*) FILTER (WHERE trade_state = 1) AS active_listings,
                       COUNT(*) AS total_listings,
                       COALESCE(ROUND(AVG(starting_price) FILTER (WHERE trade_state = 1)), 0) AS avg_start_price,
                       COALESCE(ROUND(AVG(NULLIF(highest_bid,0)) FILTER (WHERE trade_state = 1)), 0) AS avg_high_bid,
                       COALESCE(SUM(buy_out_price) FILTER (WHERE trade_state = 1), 0) AS sum_buyout
                FROM hut_trade_info
            """))[0];

            return Results.Json(new
            {
                managers = pucks["managers"],
                totalPucks = pucks["total_pucks"],
                avgPucks = pucks["avg_pucks"],
                maxPucks = pucks["max_pucks"],
                activeListings = market["active_listings"],
                totalListings = market["total_listings"],
                avgStartingPrice = market["avg_start_price"],
                avgHighestBid = market["avg_high_bid"],
                sumBuyout = market["sum_buyout"]
            });
        });
        
        var rawAllowed = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "hut_general_info", "hut_version_info", "hut_gamer_info",
            "hut_squad_info", "hut_cards", "hut_name_reservations",
            "hut_trade_info", "hut_offer_info", "hut_watching",
            tPlayers, tKits, tBadges, tTraining, tContracts,
            tHealing, tCoaches, tLeagues, tStadiums
        };

        app.MapGet($"{prefix}/api/hut/raw/{{table}}", async (string table, int? limit) =>
        {
            if (!rawAllowed.Contains(table))
                return Results.NotFound(new { message = "Unknown or non-whitelisted table" });

            int max = Math.Clamp(limit ?? 200, 1, 2000);

            await using var conn = new NpgsqlConnection(game.DatabaseConnectionString);
            await conn.OpenAsync();

            return Results.Json(await DbUtils.ReadRows(conn,
                $"SELECT * FROM {Ident(table)} LIMIT {max}"));
        });
    }
}
