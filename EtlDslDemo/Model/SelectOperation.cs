namespace EtlDsl.Model;

public class SelectOperation : IOperation
{
    public List<(string Target, string Expression)> Assignments { get; set; }
}