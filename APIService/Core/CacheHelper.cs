using StackExchange.Redis;

namespace APIService.Core;

public static class CacheHelper
{
    public static async Task<string> GetOrComputeJsonAsync(
        IDatabase? redis,
        string key,
        TimeSpan? ttl,
        Func<Task<string>> compute)
    {
        if (redis != null && ttl != null)
        {
            var cached = await redis.StringGetAsync(key);
            if (cached.HasValue)
                return cached!;
        }

        var json = await compute();

        if (redis != null && ttl != null)
            await redis.StringSetAsync(key, json, ttl.Value);

        return json;
    }
}