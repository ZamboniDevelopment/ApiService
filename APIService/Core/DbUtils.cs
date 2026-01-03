using Npgsql;

namespace APIService.Core;

public static class DbUtils
{
    public static async Task<List<Dictionary<string, object?>>> ReadRows(
        NpgsqlConnection conn,
        string sql,
        params NpgsqlParameter[] p)
    {
        using var cmd = new NpgsqlCommand(sql, conn);
        foreach (var x in p)
            cmd.Parameters.Add(x);

        var list = new List<Dictionary<string, object?>>();
        using var r = await cmd.ExecuteReaderAsync();

        while (await r.ReadAsync())
        {
            var row = new Dictionary<string, object?>();
            for (int i = 0; i < r.FieldCount; i++)
                row[r.GetName(i)] = r.IsDBNull(i) ? null : r.GetValue(i);
            list.Add(row);
        }

        return list;
    }

    public static DateTime RangeToDate(string range) =>
        range switch
        {
            "day" => DateTime.UtcNow.AddDays(-1),
            "week" => DateTime.UtcNow.AddDays(-7),
            "month" => DateTime.UtcNow.AddMonths(-1),
            _ => DateTime.MinValue
        };
}