using Antlr4.Runtime;
using EtlDsl.Executor;
using EtlDsl.Model;
using EtlDsl.Executor; //java -jar "C:\Users\ACER\Downloads\antlr4-4.13.1-complete.jar" -Dlanguage=CSharp -visitor -o Generated Grammar\EtlDsl.g4

// ---------------- 1️⃣ Define your DSL ----------------
var dsl = @"
PIPELINE TestComplex VERSION 1.0

EXTRACT CSV ""Data/sales.csv"" AS sales

TRANSFORM {
    FILTER (sales.quantity * 2 + sales.price > 150 AND NOT sales.quantity < 5) OR sales.price > 300
}

LOAD SQL ""FactComplex""

";

// ---------------- 2️⃣ Parse the DSL ----------------
var input = new AntlrInputStream(dsl);
var lexer = new EtlDslLexer(input);
var tokens = new CommonTokenStream(lexer);
var parser = new EtlDslParser(tokens);
var tree = parser.pipeline();

// ---------------- 3️⃣ Build pipeline ----------------
var visitor = new EtlDslVisitorImpl();
Pipeline pipeline = visitor.Build(tree);

// ---------------- 4️⃣ Show pipeline summary ----------------
Console.WriteLine($"Pipeline: {pipeline.Name} v{pipeline.Version}");
Console.WriteLine($"Sources: {string.Join(", ", pipeline.Extract.Sources)}");
Console.WriteLine($"Target: {pipeline.Load.Target}");
Console.WriteLine("\nTransform Operations:");
foreach (var op in pipeline.Transform.Operations)
    Console.WriteLine($"- {op.GetType().Name}");

// ---------------- 5️⃣ Run Fake ETL ----------------
Console.WriteLine("\n--- Running Fake ETL ---");
FakeEtlExecutor.Run(pipeline);