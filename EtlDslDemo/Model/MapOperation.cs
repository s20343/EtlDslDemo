namespace EtlDsl.Model;

public class MapOperation : Operation
{
    public string Expression { get; set; } = "";
    public string TargetColumn { get; set; } = "";
    public DataType? TargetType { get; set; }
}