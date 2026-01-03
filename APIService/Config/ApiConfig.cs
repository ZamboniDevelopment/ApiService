using Microsoft.Extensions.Configuration;

namespace APIService.Config;

public class ApiConfig
{
    public GeneralConfig General { get; set; } = new();
    public Dictionary<string, GameConfig> Games { get; set; } = new();

    public static ApiConfig Load(IConfiguration cfg)
    {
        var c = new ApiConfig();
        cfg.Bind(c);
        return c;
    }
}

public class GeneralConfig
{
    public string RedisConnectionString { get; set; } = "";
    public string IP { get; set; } = "auto";
    public int Port { get; set; } = 5000;
}

public enum GameType
{
    NHLLegacy,
    NHL14,
    NHL11,
    NHL10
}

public class GameConfig
{
    public bool Enabled { get; set; }
    public string DatabaseConnectionString { get; set; } = "";
    public string RoutePrefix { get; set; } = "";
    public GameType Type { get; set; }
}