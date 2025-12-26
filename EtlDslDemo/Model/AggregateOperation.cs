namespace EtlDsl.Model;

public class AggregateOperation : IOperation
{
    public string Expression { get; set; }
    public string Function { get; set; } // SUM, AVG, MIN, MAX
    public List<string> GroupByColumns { get; set; } = new();
    public string TargetColumn { get; set; }
    public DataType? TargetType { get; set; }
}