namespace EtlDsl.Model;

public class AggregateOperation : Operation
{
    public string Function { get; set; } = "";  // SUM or AVG
    public string Expression { get; set; } = "";
    public List<string> GroupByColumns { get; set; } = new();
    public string TargetColumn { get; set; } = "";
    public DataType? TargetType { get; set; }
}