namespace EtlDsl.Model;

public class LoadStep
{
    public string TargetType { get; set; } = "";       // SQL etc.
    public string Target { get; set; } = "";           // table name or file
    public List<FlatFileOut> FlatFiles { get; set; } = new();
    public List<OutObject> OutObjects { get; set; } = new();
 
    public List<object> OutputStreams { get; set; } = new(); // FlatFileOut or OutObject


}