using Antlr4.Runtime;
using EtlDsl.Model;
using EtlDsl.Executor;
//java -jar "C:\Users\ACER\Downloads\antlr4-4.13.1-complete.jar" -Dlanguage=CSharp -visitor -o Generated Grammar\EtlDsl.g4
var dsl = @"
PIPELINE FullTestAll VERSION 1.0

EXTRACT csv ""Data/sales.csv"", ""Data/discount.csv"" AS sales

TRANSFORM {
    MAP sales.quantity TO QtyCopy AS INT
    MAP IF sales.price > 5 THEN sales.price * 0.9 ELSE sales.price TO AdjustedPrice AS DECIMAL
    FILTER sales.quantity >= 0
    AGGREGATE SUM(sales.quantity) AS TotalQty GROUPBY sales.product
    AGGREGATE AVG(sales.price) AS AvgPrice GROUPBY sales.product
    AGGREGATE MIN(sales.price) AS MinPrice GROUPBY sales.product
    AGGREGATE MAX(sales.price) AS MaxPrice GROUPBY sales.product
}

LOAD sql ""FactSalesAgg""







";




var input = new AntlrInputStream(dsl);
var lexer = new EtlDslLexer(input);
var tokens = new CommonTokenStream(lexer);
var parser = new EtlDslParser(tokens);

var tree = parser.pipeline();

var visitor = new EtlDslVisitorImpl();
Pipeline pipeline = visitor.Build(tree);

Console.WriteLine($"Pipeline: {pipeline.Name} v{pipeline.Version}");
Console.WriteLine($"Source: {pipeline.Extract.Sources}");
Console.WriteLine($"Target: {pipeline.Load.Target}");


// run fake etl
FakeEtlExecutor.Run(pipeline);