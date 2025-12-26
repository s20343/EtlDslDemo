namespace EtlDsl.Model;

public class ExtractStep
{
    public string Alias { get; set; }               // e.g., benchPositionFileStream
    public List<string> Sources { get; set; }      // CSV or FLATFILE paths
}
