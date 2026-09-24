using APIService.Config;
using APIService.Games.HUT;
using APIService.Games.NHL;

namespace APIService;

public static class GameCatalog
{
    private static readonly Dictionary<GameType, Action<WebApplication, GameConfig>> Handlers = new()
    {
        [GameType.Nhl10] = (app, g) => NhlEndpoints.Map(app, g, NhlSchemas.Nhl10),
        [GameType.Nhl11] = (app, g) => NhlEndpoints.Map(app, g, NhlSchemas.Nhl11),
        [GameType.Nhl12] = (app, g) => NhlEndpoints.Map(app, g, NhlSchemas.Nhl12),
        [GameType.Nhl13] = (app, g) => NhlEndpoints.Map(app, g, NhlSchemas.Shared),
        [GameType.Nhl14] = (app, g) => NhlEndpoints.Map(app, g, NhlSchemas.Shared),
        [GameType.Nhl15] = (app, g) => NhlEndpoints.Map(app, g, NhlSchemas.Shared),
        [GameType.NhlLegacy] = (app, g) => NhlEndpoints.Map(app, g, NhlSchemas.Shared),
        [GameType.Hut] = HutApi.Map,
    };

    public static void Map(WebApplication app, GameConfig game)
    {
        if (Handlers.TryGetValue(game.Type, out var handler))
            handler(app, game);
    }
}