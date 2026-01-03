using StackExchange.Redis;

namespace APIService.Core;

public static class RedisUtils
{
    public static IDatabase? GetDatabase(HttpContext ctx)
    {
        try
        {
            var mux = ctx.RequestServices
                .GetRequiredService<IConnectionMultiplexer>();
            return mux.GetDatabase();
        }
        catch
        {
            return null;
        }
    }
}