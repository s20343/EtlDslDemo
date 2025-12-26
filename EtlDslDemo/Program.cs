using Antlr4.Runtime;
using EtlDsl.Executor;
using EtlDsl.Model;
using EtlDsl.Executor; //java -jar "C:\Users\ACER\Downloads\antlr4-4.13.1-complete.jar" -Dlanguage=CSharp -visitor -o Generated Grammar\EtlDsl.g4

// ---------------- 1️⃣ Define your DSL ----------------
var dsl = @"

PIPELINE TestValid VERSION 1.0

EXTRACT CSV ""Data/sales.csv"" AS sales

TRANSFORM {
    MAP sales.quantity * sales.price TO total
    FILTER sales.price > 100
    AGGREGATE SUM(total) AS sum_total GROUPBY sales.category
}

LOAD SQL ""FactValid""

";


// 2️⃣ Parse the DSL
var input = new AntlrInputStream(dsl);
var lexer = new EtlDslLexer(input);
var tokens = new CommonTokenStream(lexer);
var parser = new EtlDslParser(tokens);
var tree = parser.pipeline();

// 3️⃣ Build the pipeline
var visitor = new EtlDslVisitorImpl();
Pipeline pipeline = visitor.Build(tree);

// 4️⃣ Validate the pipeline
try
{
    PipelineValidator.Validate(pipeline);
    Console.WriteLine("Pipeline validation passed ✅");
}
catch (Exception ex)
{
    Console.WriteLine($"Pipeline validation failed ❌: {ex.Message}");
}

// 5️⃣ (Optional) Run Fake ETL if validation passed
FakeEtlExecutor.Run(pipeline);