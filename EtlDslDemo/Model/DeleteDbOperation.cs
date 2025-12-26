namespace EtlDsl.Model;

public class DeleteDbOperation : IOperation
{

    public string TargetTable { get; set; }          // BenchmarkComposition
    public string Condition { get; set; }            // FundCode = Portfolio.InternalCode AND Date >= Date
}