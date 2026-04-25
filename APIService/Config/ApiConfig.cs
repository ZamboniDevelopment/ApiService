using System.ComponentModel.DataAnnotations;

namespace APIService.Config;

public class ApiConfig
{
    [Required]
    public GeneralConfig General { get; set; } = new();

    [Required]
    public Dictionary<string, GameConfig> Games { get; set; } = new();
}

public class GeneralConfig
{
    [Required]
    public string RedisConnectionString { get; set; } = "";

    [Required]
    public string IP { get; set; } = "auto";

    [Range(1, 65535)]
    public int Port { get; set; } = 5000;
}

public enum GameType
{
    NhlLegacy,
    Nhl15,
    Nhl14,
    Nhl13,
    Nhl12,
    Nhl11,
    Nhl10
}

public class GameConfig
{
    public bool Enabled { get; set; }

    [Required]
    public string DatabaseConnectionString { get; set; } = "";

    [Required]
    public string RoutePrefix { get; set; } = "";

    [Required]
    public GameType Type { get; set; }
}