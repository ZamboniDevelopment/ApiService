using System.Collections.Concurrent;

namespace APIService.Core;

public class FixedWindowRateLimiter
{
    private readonly int _permitLimit;
    private readonly TimeSpan _window;
    private readonly int _queueLimit;
    private readonly ConcurrentQueue<DateTime> _requests = new();

    public FixedWindowRateLimiter(int permitLimit, TimeSpan window, int queueLimit)
    {
        _permitLimit = permitLimit;
        _window = window;
        _queueLimit = queueLimit;
    }

    public Task<bool> AllowRequestAsync()
    {
        var now = DateTime.UtcNow;

        while (_requests.TryPeek(out var ts) && now - ts > _window)
            _requests.TryDequeue(out _);

        if (_requests.Count >= _permitLimit + _queueLimit)
            return Task.FromResult(false);

        _requests.Enqueue(now);
        return Task.FromResult(true);
    }
}