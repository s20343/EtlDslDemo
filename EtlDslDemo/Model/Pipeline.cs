namespace EtlDsl.Model;

public class Pipeline
{
    public string Name { get; set; } = "";
    public string Version { get; set; } = "";
    public ExtractStep Extract { get; set; } = null!;
    public TransformStep Transform { get; set; } = null!;
    public LoadStep Load { get; set; } = null!;
}