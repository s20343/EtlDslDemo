using EtlDsl.Model;

public class EtlDslVisitorImpl : EtlDslBaseVisitor<object>
{
    private readonly Pipeline _pipeline = new();
    private TransformStep? _currentTransform;

    public Pipeline Build(EtlDslParser.PipelineContext ctx)
    {
        Visit(ctx);
        return _pipeline;
    }

    public override object VisitPipeline(EtlDslParser.PipelineContext ctx)
    {
        _pipeline.Name = ctx.IDENTIFIER().GetText();
        _pipeline.Version = ctx.NUMBER().GetText();
        Visit(ctx.extract());
        Visit(ctx.transform());
        Visit(ctx.load());
        return _pipeline;
    }

    public override object VisitExtract(EtlDslParser.ExtractContext ctx)
    {
        var sources = ctx.sourceList().STRING()
            .Select(s => TrimQuotes(s.GetText()))
            .ToList();

        _pipeline.Extract = new ExtractStep
        {
            SourceType = ctx.sourceType().GetText(),
            Sources = sources,
            Alias = ctx.IDENTIFIER().GetText()  // Keep alias!
        };
        return null!;
    }

    public override object VisitTransform(EtlDslParser.TransformContext ctx)
    {
        _currentTransform = new TransformStep();
        foreach (var stmt in ctx.transformStatement())
            Visit(stmt);

        _pipeline.Transform = _currentTransform;
        return null!;
    }

    public override object VisitMapStatement(EtlDslParser.MapStatementContext ctx)
    {
        if (ctx.IF() != null)
        {
            _currentTransform!.Operations.Add(new ConditionalMapOperation
            {
                Condition = ctx.expression(0).GetText(),
                TrueExpression = ctx.expression(1).GetText(),
                FalseExpression = ctx.expression(2).GetText(),
                TargetColumn = ctx.IDENTIFIER().GetText(),
                TargetType = ctx.type() != null ? ParseType(ctx.type()!.GetText()) : null
            });

            return null!;
        }

        _currentTransform!.Operations.Add(new MapOperation
        {
            Expression = ctx.expression(0).GetText(),
            TargetColumn = ctx.IDENTIFIER().GetText(),
            TargetType = ctx.type() != null ? ParseType(ctx.type()!.GetText()) : null
        });

        return null!;
    }
    
    

    public override object VisitAggregateStatement(EtlDslParser.AggregateStatementContext ctx)
    {
        var agg = new AggregateOperation
        {
            Function = ctx.aggregationFunction().GetText(),
            Expression = ctx.expression().GetText(),
            TargetColumn = ctx.targetIdentifier().GetText(),
            TargetType = ctx.type() != null ? ParseType(ctx.type().GetText()) : DataType.Decimal
        };

        if (ctx.groupByClause() != null)
        {
            foreach (var item in ctx.groupByClause().groupByItem())
            {
                agg.GroupByColumns.Add(item.GetText());
            }
        }


        _currentTransform!.Operations.Add(agg);
        return null!;
    }

    public override object VisitFilterStatement(EtlDslParser.FilterStatementContext ctx)
    {
        _currentTransform!.Operations.Add(new FilterOperation
        {
            Condition = ctx.expression().GetText()
        });
        return null!;
    }

    public override object VisitLoad(EtlDslParser.LoadContext ctx)
    {
        _pipeline.Load = new LoadStep
        {
            TargetType = ctx.targetType().GetText(),
            Target = TrimQuotes(ctx.STRING().GetText())
        };
        return null!;
    }

    private static DataType ParseType(string text) =>
        text switch
        {
            "INT" => DataType.Int,
            "DECIMAL" => DataType.Decimal,
            "STRING" => DataType.String,
            _ => throw new Exception($"Unknown type {text}")
        };

    private static string TrimQuotes(string s) => s.Trim('"');
}
