using Antlr4.Runtime;
using Antlr4.Runtime.Misc;
using EtlDsl.Model;
using System;
using System.Linq;

namespace EtlDsl.Executor
{
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
            var extractStep = new ExtractStep();

            foreach (var srcCtx in ctx.extractSource())
            {
                var path = TrimQuotes(srcCtx.STRING().GetText());
                var alias = srcCtx.IDENTIFIER().GetText();

                extractStep.SourcesWithAlias.Add(new ExtractSource
                {
                    Path = path,
                    Alias = alias
                });
            }

            _pipeline.Extract = extractStep;
            return null!;
        }
        

        // ---------------- Transform ----------------
        // EtlDslVisitorImpl.cs
        public override object VisitTransform(EtlDslParser.TransformContext ctx)
        {
            _currentTransform = new TransformStep();
            _pipeline.Transform = _currentTransform;
            _pipeline.Transform.SourceBlocks = new List<SourceTransformBlock>(); // initialize

            foreach (var stmt in ctx.transformStatementOrBlock())
            {
                if (stmt.sourceTransformBlock() != null)
                {
                    Visit(stmt.sourceTransformBlock());
                }
                else if (stmt.transformStatement() != null)
                {
                    var op = Visit(stmt.transformStatement()) as IOperation;
                    if (op != null)
                        _currentTransform.Operations.Add(op); // Add to global operations
                }
            }

            return null!;
        }

        public override object VisitSourceTransformBlock(EtlDslParser.SourceTransformBlockContext ctx)
        {
            var block = new SourceTransformBlock
            {
                SourceAlias = ctx.IDENTIFIER().GetText(),
                Operations = new List<IOperation>()
            };

            foreach (var stmt in ctx.transformStatement())
            {
                var op = Visit(stmt) as IOperation;
                if (op != null)
                {
                    // Auto-prefix the target column
                    if (op is MapOperation map)
                        map.TargetColumn = $"{block.SourceAlias}.{map.TargetColumn}";
                    else if (op is ConditionalMapOperation cond)
                        cond.TargetColumn = $"{block.SourceAlias}.{cond.TargetColumn}";
                    //else if (op is AggregateOperation agg)
                        //agg.TargetColumn = $"{block.SourceAlias}.{agg.TargetColumn}";

                    block.Operations.Add(op);
                }
            }

            _pipeline.Transform.SourceBlocks.Add(block);
            return null!;
        }


        // ---------------- Map ----------------
        public override object VisitMapStatement(EtlDslParser.MapStatementContext ctx)
        {
            if (_currentTransform == null) return null!;

            if (ctx.IF() != null)
            {
                return new ConditionalMapOperation
                {
                    Condition = GetFullText(ctx.expression(0)),
                    TrueExpression = GetFullText(ctx.expression(1)),
                    FalseExpression = GetFullText(ctx.expression(2)),
                    TargetColumn = ctx.IDENTIFIER().GetText(),
                    TargetType = ctx.type() != null ? ParseType(ctx.type().GetText()) : null
                };
            }

            return new MapOperation
            {
                Expression = GetFullText(ctx.expression(0)),
                TargetColumn = ctx.IDENTIFIER().GetText(),
                TargetType = ctx.type() != null ? ParseType(ctx.type().GetText()) : null
            };
        }

        // ---------------- Filter ----------------
        public override object VisitFilterStatement(EtlDslParser.FilterStatementContext ctx)
        {
            return new FilterOperation
            {
                Condition = GetFullText(ctx.expression())
            };
        }

        // ---------------- Aggregate ----------------
        public override object VisitAggregateStatement(EtlDslParser.AggregateStatementContext ctx)
        {
            var agg = new AggregateOperation
            {
                Function = ctx.aggregationFunction().GetText(),
                Expression = GetFullText(ctx.expression()),
                TargetColumn = ctx.targetIdentifier().GetText(),
                TargetType = ctx.type() != null ? ParseType(ctx.type().GetText()) : null
            };

            if (ctx.groupByClause() != null)
            {
                agg.GroupByColumns = ctx.groupByClause().groupByItem()
                    .Select(g => g.GetText())
                    .ToList();
            }

            return agg;
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
        private string GetFullText(ParserRuleContext context)
        {
            if (context == null) return "";
            int a = context.Start.StartIndex;
            int b = context.Stop.StopIndex;
            var interval = new Interval(a, b);
            return context.Start.InputStream.GetText(interval);
        }

        private static DataType ParseType(string text) =>
            text.Trim().ToLowerInvariant() switch
            {
                "int" => DataType.Int,
                "num" => DataType.Int,
                "double" => DataType.Decimal,
                "string" => DataType.String,
                "boolean" => DataType.Boolean,
                _ => throw new Exception($"Unknown type {text}")
            };

        private static string TrimQuotes(string s) => s.Trim('"');
    }
}
