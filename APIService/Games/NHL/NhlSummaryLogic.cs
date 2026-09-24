using APIService.Core;

namespace APIService.Games.NHL;

public static class NhlSummaryLogic
{
    public sealed record GameSummaryResult(
        string? HomeTeam, string? AwayTeam, int HomeScore, int AwayScore, string? WinnerTeam);
    
    public static GameSummaryResult ComputeGameSummary(IEnumerable<Dictionary<string, object?>> reportRows)
    {
        var rows = reportRows.ToList();

        var home = rows.Where(r => Convert.ToBoolean(r.GetValueOrDefault("home_ind"))).ToList();
        var away = rows.Where(r => !Convert.ToBoolean(r.GetValueOrDefault("home_ind"))).ToList();

        string? homeTeam = home.Select(r => r.GetValueOrDefault("team_name")?.ToString()).FirstOrDefault(x => x != null);
        string? awayTeam = away.Select(r => r.GetValueOrDefault("team_name")?.ToString()).FirstOrDefault(x => x != null);
        int homeScore = home.Sum(r => Helper.I(r.GetValueOrDefault("score")));
        int awayScore = away.Sum(r => Helper.I(r.GetValueOrDefault("score")));

        string? winnerTeam =
            homeScore > awayScore ? homeTeam :
            awayScore > homeScore ? awayTeam :
            null;

        return new GameSummaryResult(homeTeam, awayTeam, homeScore, awayScore, winnerTeam);
    }
    
    public static (Dictionary<string, (int Games, int Goals)> PerSource, int TotalGames, int TotalGoals)
        ComputePlayerBreakdown(IEnumerable<(string JsonKey, List<Dictionary<string, object?>> Rows)> perSource)
    {
        var list = perSource.ToList();

        int TotalGoalsFor(List<Dictionary<string, object?>> rows) =>
            rows.Sum(r => Helper.I(r.GetValueOrDefault("score")));

        var breakdown = list.ToDictionary(s => s.JsonKey, s => (s.Rows.Count, TotalGoalsFor(s.Rows)));

        return (breakdown, list.Sum(s => s.Rows.Count), list.Sum(s => TotalGoalsFor(s.Rows)));
    }
}