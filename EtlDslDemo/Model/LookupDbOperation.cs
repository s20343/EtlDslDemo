namespace EtlDsl.Model;

public class LookupDbOperation : IOperation
{
    public LookupOperation Lookup { get; set; }
    public string TargetTable { get; set; }
    public List<(string Left, string Right)> On { get; set; } = new();
    public List<(string Target, string Expression)> SelectColumns { get; set; } = new();
    public bool FullCache { get; set; } = false;
}