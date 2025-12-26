namespace EtlDsl.Model;

public class MapOperation : IOperation
{
    public string Expression { get; set; }
    public string TargetColumn { get; set; }
    public DataType? TargetType { get; set; }
}