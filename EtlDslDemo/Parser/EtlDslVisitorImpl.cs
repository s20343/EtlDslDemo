using EtlDsl.Model;
using System.Linq;
namespace EtlDsl.Executor;

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

    // ---------------- Extract ----------------
    public override object VisitExtract(EtlDslParser.ExtractContext ctx)
    {
        var sources = ctx.sourceList().STRING()
            .Select(s => TrimQuotes(s.GetText()))
            .ToList();

        _pipeline.Extract = new ExtractStep
        {
            //SourceType = ctx.sourceType().GetText(),
            Sources = sources,
            Alias = ctx.targetIdentifier().GetText()
        };
        return null!;
    }

    // ---------------- Transform ----------------
    public override object VisitTransform(EtlDslParser.TransformContext ctx)
    {
        _currentTransform = new TransformStep();
        foreach (var stmt in ctx.transformStatement())
        {
            Visit(stmt);
        }
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
                TargetType = ctx.type() != null ? ParseType(ctx.type().GetText()) : null
            });
            return null!;
        }

        _currentTransform!.Operations.Add(new MapOperation
        {
            Expression = ctx.expression(0).GetText(),
            TargetColumn = ctx.IDENTIFIER().GetText(),
            TargetType = ctx.type() != null ? ParseType(ctx.type().GetText()) : null
        });
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

    public override object VisitDistinctStatement(EtlDslParser.DistinctStatementContext ctx)
    {
        _currentTransform!.Operations.Add(new DistinctOperation
        {
            Columns = ctx.expressionList().expression()
                .Select(e => e.GetText())
                .ToList()
        });
        return null!;
    }

    public override object VisitDeleteDb(EtlDslParser.DeleteDbContext ctx)
    {
        _currentTransform!.Operations.Add(new DeleteDbOperation
        {
            Condition = ctx.expression().GetText()
        });
        return null!;
    }

    public override object VisitLookupObStatement(EtlDslParser.LookupObStatementContext ctx)
    {
        var lookup = Visit(ctx.lookupStatement()) as LookupOperation;
        _currentTransform!.Operations.Add(new LookupOperation { TargetTable = lookup!.TargetTable, On = lookup.On });
        return null!;
    }

    public override object VisitLookupDbStatement(EtlDslParser.LookupDbStatementContext ctx)
    {
        var lookup = Visit(ctx.lookupStatement()) as LookupOperation;
        _currentTransform!.Operations.Add(new LookupDbOperation { Lookup = lookup! });
        return null!;
    }

    public override object VisitSelectStatement(EtlDslParser.SelectStatementContext ctx)
    {
        var select = new SelectOperation
        {
            Assignments = ctx.assignmentList().assignment()
                .Select(a => (a.IDENTIFIER().GetText(), a.expression().GetText()))
                .ToList()
        };
        _currentTransform!.Operations.Add(select);
        return null!;
    }

    public override object VisitSelectDbStatement(EtlDslParser.SelectDbStatementContext ctx)
    {
        _currentTransform!.Operations.Add(new SelectDbOperation());
        return null!;
    }

    public override object VisitCorrelateStatement(EtlDslParser.CorrelateStatementContext ctx)
    {
        _currentTransform!.Operations.Add(new CorrelateOperation());
        return null!;
    }

    public override object VisitSynchronizedStatement(EtlDslParser.SynchronizedStatementContext ctx)
    {
        _currentTransform!.Operations.Add(new SynchronizedOperation());
        return null!;
    }

    public override object VisitCrossApplyStatement(EtlDslParser.CrossApplyStatementContext ctx)
    {
        _currentTransform!.Operations.Add(new CrossApplyOperation());
        return null!;
    }

    public override object VisitLookupStatement(EtlDslParser.LookupStatementContext ctx)
    {
        var lookup = new LookupOperation
        {
            TargetTable = ctx.expression().GetText(),
            On = new List<(string Left, string Right)>()
        };

        var a = ctx.assignment(); // single AssignmentContext
        if (a != null)
        {
            lookup.On.Add((Left: a.IDENTIFIER().GetText(), Right: a.expression().GetText()));
        }

        return lookup;
    }
    

    // ---------------- Load ----------------
    public override object VisitLoad(EtlDslParser.LoadContext ctx)
    {
        _pipeline.Load = new LoadStep
        {
            TargetType = ctx.targetType().GetText(),
            Target = TrimQuotes(ctx.STRING().GetText())
        };
        return null!;
    }

    // ---------------- Helpers ----------------
    private static DataType ParseType(string text) =>
        text.ToUpper() switch
        {
            "INT"     => DataType.Int,
            "NUM"     => DataType.Int,        // match your lexer token
            "DECIMAL" => DataType.Decimal,
            "DOUBLE"  => DataType.Decimal,    // or DataType.Double if you have it
            "STRING"  => DataType.String,
            "BOOLEAN" => DataType.Boolean,
            _ => throw new Exception($"Unknown type {text}")
        };


    private static string TrimQuotes(string s) => s.Trim('"');
}
