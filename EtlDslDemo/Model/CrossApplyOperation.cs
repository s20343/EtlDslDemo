namespace EtlDsl.Model;

public class CrossApplyOperation : IOperation
{
    public IOperation Operation { get; set; }        // Could be any operation (Map, Lookup, DeleteDb...)
}