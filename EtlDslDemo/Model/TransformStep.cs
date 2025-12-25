namespace EtlDsl.Model;

public class TransformStep : Step
{
    public List<Operation> Operations { get; } = new();
}