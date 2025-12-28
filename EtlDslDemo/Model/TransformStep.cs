namespace EtlDsl.Model;

public class TransformStep
{
    // Flat list of operations (for single-source pipelines / backward compatibility)
    public List<IOperation> Operations { get; set; } = new();

    // NEW: per-source transform blocks
    public List<SourceTransformBlock> SourceBlocks { get; set; } = new();
}

