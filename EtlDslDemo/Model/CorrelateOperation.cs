namespace EtlDsl.Model;


public class CorrelateOperation : IOperation
{
    public string Source { get; set; }                // e.g., benchCompositionStream
    public List<(string Target, string Expression)> Assignments { get; set; } = new();
    public string Alias { get; set; }                 // target alias
}