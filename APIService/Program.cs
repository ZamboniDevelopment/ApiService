using APIService.Config;
using APIService.Core;
using APIService.Games.NHL10;
using APIService.Games.NHL11;
using APIService.Games.NHL14Legacy;
using StackExchange.Redis;

namespace APIService;

internal class Program
{
    private static async Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        var config = ApiConfig.Load(builder.Configuration);
        builder.Services.AddSingleton(config);

        builder.Services.AddSingleton<ConnectionMultiplexer>(sp =>
        {
            try
            {
                return ConnectionMultiplexer.Connect(
                    config.General.RedisConnectionString
                );
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Redis] Connection to Redis failed. {ex.Message}");
                var options = ConfigurationOptions.Parse(
                    config.General.RedisConnectionString
                );
                options.AbortOnConnectFail = false;
                return ConnectionMultiplexer.Connect(options);
            }
        });

        builder.Services.AddSingleton<FixedWindowRateLimiter>(_ =>
            new FixedWindowRateLimiter(
                permitLimit: 120,
                window: TimeSpan.FromMinutes(1),
                queueLimit: 10
            ));

        builder.Services.AddEndpointsApiExplorer();

        var app = builder.Build();

        string ip = config.General.IP == "auto"
            ? "0.0.0.0"
            : config.General.IP;

        app.Urls.Add($"http://{ip}:{config.General.Port}");

        app.Use(async (ctx, next) =>
        {
            var limiter = ctx.RequestServices
                .GetRequiredService<FixedWindowRateLimiter>();

            if (!await limiter.AllowRequestAsync())
            {
                ctx.Response.StatusCode = StatusCodes.Status429TooManyRequests;
                await ctx.Response.WriteAsJsonAsync(new
                {
                    message = "Too many requests"
                });
                return;
            }

            await next();
        });
        
        foreach (var game in config.Games.Values.Where(g => g.Enabled))
        {
            switch (game.Type)
            {
                case GameType.NHLLegacy:
                case GameType.NHL14:
                    NHLSharedApi.Map(app, game);
                    break;

                case GameType.NHL11:
                    NHL11Api.Map(app, game);
                    break;

                case GameType.NHL10:
                    Nhl10Api.Map(app, game);
                    break;
            }
        }

        await app.RunAsync();
    }
}
