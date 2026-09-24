using APIService.Config;
using APIService.Core;
using StackExchange.Redis;
using Microsoft.Extensions.Options;

namespace APIService;

internal class Program
{
    private static async Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
        builder.Services
            .AddOptions<ApiConfig>()
            .Bind(builder.Configuration)
            .ValidateDataAnnotations()
            .Validate(cfg =>
            {
                foreach (var game in cfg.Games.Values.Where(g => g.Enabled))
                {
                    if (string.IsNullOrWhiteSpace(game.DatabaseConnectionString))
                        return false;

                    if (string.IsNullOrWhiteSpace(game.RoutePrefix))
                        return false;
                }

                return true;
            }, "Invalid game configuration");

        // Redis
        builder.Services.AddSingleton<ConnectionMultiplexer>(sp =>
        {
            var config = sp
                .GetRequiredService<IOptions<ApiConfig>>()
                .Value;

            try
            {
                return ConnectionMultiplexer.Connect(
                    config.General.RedisConnectionString
                );
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Redis] Initial connection failed: {ex.Message}");

                var options = ConfigurationOptions.Parse(
                    config.General.RedisConnectionString
                );

                options.AbortOnConnectFail = false;
                return ConnectionMultiplexer.Connect(options);
            }
        });

        // Rate limits
        builder.Services.AddSingleton<FixedWindowRateLimiter>(_ =>
            new FixedWindowRateLimiter(
                permitLimit: 120,
                window: TimeSpan.FromMinutes(1),
                queueLimit: 10
            ));
        builder.Services.AddEndpointsApiExplorer();
        var app = builder.Build();
        var config = app.Services
            .GetRequiredService<IOptions<ApiConfig>>()
            .Value;
        var ip = config.General.IP == "auto"
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

        // Map games
        foreach (var game in config.Games.Values.Where(g => g.Enabled))
        {
            GameCatalog.Map(app, game);
        }
        await app.RunAsync();
    }
}
