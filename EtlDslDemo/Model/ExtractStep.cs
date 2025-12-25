namespace EtlDsl.Model;

public class ExtractStep
{
    public string SourceType { get; set; } = "";
    public List<string> Sources { get; set; } = new();
    public string Alias { get; set; } = "";
}