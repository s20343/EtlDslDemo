namespace EtlDsl.Model;

public class ExtractStep
{
    // NEW (preferred)
    public List<ExtractSource> SourcesWithAlias { get; set; } = new();

    // OLD (kept for backward compatibility if needed)
    public string Alias { get; set; }
    public List<string> Sources { get; set; }
}

