namespace APIService.Core;

public class Helper
{
    //32->64
    public static long L(object? v)
        => v == null || v == DBNull.Value ? 0L : Convert.ToInt64(v);

    // 64->32
    public static int I(object? v)
        => v == null || v == DBNull.Value ? 0 : Convert.ToInt32(v);
}