namespace EtlDsl.Model;

public class SourceTransformBlock
{
    public string SourceAlias { get; set; } = "";
    public List<IOperation> Operations { get; set; } = new();
}