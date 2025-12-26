namespace EtlDsl.Model;

public class ConditionalMapOperation : IOperation
{
    public string Condition { get; set; }
    public string TrueExpression { get; set; }
    public string FalseExpression { get; set; }
    public string TargetColumn { get; set; }
    public DataType? TargetType { get; set; }
}