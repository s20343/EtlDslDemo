using Antlr4.Runtime;
using Antlr4.Runtime.Misc; // 1️⃣ Required for Interval
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
            var sources = ctx.sourceList().STRING()
                .Select(s => TrimQuotes(s.GetText()))
                .ToList();

            _pipeline.Extract = new ExtractStep
            {
                Sources = sources,
                Alias = ctx.targetIdentifier().GetText()
            };
            return null!;
        }

        // ---------------- Transform ----------------
        public override object VisitTransform(EtlDslParser.TransformContext ctx)
        {
            _currentTransform = new TransformStep();
            _pipeline.Transform = _currentTransform;
            foreach (var stmt in ctx.transformStatement())
            {
                Visit(stmt);
            }
            return null!;
        }

        public override object VisitFilterStatement(EtlDslParser.FilterStatementContext ctx)
        {
            // 2️⃣ FIX: Use GetFullText to preserve " AND ", " OR ", " NOT " spaces
            _currentTransform!.Operations.Add(new FilterOperation
            {
                Condition = GetFullText(ctx.expression())
            });
            return null!;
        }

        public override object VisitMapStatement(EtlDslParser.MapStatementContext ctx)
        {
            if (ctx.IF() != null)
            {
                _currentTransform!.Operations.Add(new ConditionalMapOperation
                {
                    // 2️⃣ FIX: Use GetFullText here too
                    Condition = GetFullText(ctx.expression(0)),
                    TrueExpression = GetFullText(ctx.expression(1)),
                    FalseExpression = GetFullText(ctx.expression(2)),
                    TargetColumn = ctx.IDENTIFIER().GetText(),
                    TargetType = ctx.type() != null ? ParseType(ctx.type().GetText()) : null
                });
                return null!;
            }

            _currentTransform!.Operations.Add(new MapOperation
            {
                Expression = GetFullText(ctx.expression(0)),
                TargetColumn = ctx.IDENTIFIER().GetText(),
                TargetType = ctx.type() != null ? ParseType(ctx.type().GetText()) : null
            });
            return null!;
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

        // 3️⃣ THE MAGIC METHOD: Gets text with whitespace intact
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