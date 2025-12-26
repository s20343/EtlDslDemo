namespace EtlDsl.Model;

public class SynchronizedOperation : IOperation
{
    public string Source { get; set; }               // e.g., deleteNewerExistingBenchMarkCompoStream
    public string Alias { get; set; }                // e.g., waitCompositionDeletion
}