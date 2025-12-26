namespace EtlDsl.Model;

public class Pipeline
{
    public string Name { get; set; }
    public string Version { get; set; }
    public ExtractStep Extract { get; set; }
    public TransformStep Transform { get; set; }
    public LoadStep Load { get; set; }
    
    public List<TransformStep> TransformSteps { get; set; } = new List<TransformStep>();

}
