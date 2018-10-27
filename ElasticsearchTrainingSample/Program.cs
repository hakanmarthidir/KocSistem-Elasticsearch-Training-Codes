using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Nest;

namespace ElasticsearchTrainingSample
{
    public class News
    {
        public Guid NewsId { get; set; }
        public string NewsTitle { get; set; }
        public string NewsUrl { get; set; }
    }
    public class Program
    {
        static void Main(string[] args)
        {
            var elasticUrl = "http://127.0.0.1:9200";
            var connectionSettings = new ConnectionSettings(new Uri(elasticUrl)).DefaultIndex("news-deneme");
            var client = new ElasticClient(connectionSettings);

            //Step 1 - İlk once ilgili index yani veritabanı yaratılır. Veritabanı/Index yaratılırken dataların ne sekilde indexlenecegi, analiz edilecegi gibi detaylar belirlenir. 
            //CreateIndex(client);

            //Step 2 - Indexi yaratırken verilen analyzerlar kendisine indexlenmek uzere gonderilen dataları işler. 
            //DataSeed(client);

            //Step 3 - Search için hazır hale gelen datalar uzerinde karsılasılan durumlara gore cesitli sorgular calistirilabilir. 
            //QuerySample(client);

            Console.WriteLine("OK");
            Console.Read();
        }
        public static List<News> GetAllNews()
        {
            List<News> newsList = new List<News>()
            {
                new News(){ NewsId = Guid.NewGuid(), NewsTitle = "istanbulda hava durumu", NewsUrl="http://istanbul.com.tr" },
                new News(){ NewsId = Guid.NewGuid(), NewsTitle = "galatasaray da transfer", NewsUrl="http://galatasaray.com.tr" },
                new News(){ NewsId = Guid.NewGuid(), NewsTitle = "şampiyonlar ligi kuraları", NewsUrl="http://deneme.com.tr"},
            };

            return newsList;
        }
        public static void CreateIndex(ElasticClient client)
        {
            var analyzer = new CustomAnalyzer();
            analyzer.Tokenizer = "standard";
            analyzer.Filter = new List<string> { "lowercase", "asciifolding", "word_delimiter" };
            analyzer.CharFilter = new List<string> { "html_strip" };

            client.CreateIndex("news-deneme", c => c.Settings(s => s.NumberOfShards(1).
                                                                   NumberOfReplicas(1).
                                                                   Analysis(a => a.Analyzers(b => b.UserDefined("default", analyzer)))).
                                                                   Mappings(m => m.Map<News>(d => d.AutoMap())));
        }
        public static void CreateIndexWithEdgeNGram(ElasticClient client)
        {
            client.CreateIndex("edge-deneme", c => c.Settings(s => s.NumberOfShards(1).
                                                                   NumberOfReplicas(1).
                                                                   Analysis(analysis => analysis.Analyzers(analyzers => analyzers
                                        .Custom("auto-complete", a => a.Tokenizer("standard").Filters("lowercase", "asciifolding", "standard", "auto-complete-filter"))
                                        .Custom("default", a => a.Tokenizer("standard").Filters("lowercase", "asciifolding", "word_delimiter").CharFilters("html_strip")))
                                        .TokenFilters(tokenFilter => tokenFilter.EdgeNGram("auto-complete-filter", t => t.MinGram(3).MaxGram(8))))));
        }
        public static void DataSeed(ElasticClient client)
        {
            var newsList = GetAllNews();
            var waitHandle = new CountdownEvent(1);
            var bulkAll = client.BulkAll<News>(newsList, b => b
            .BackOffRetries(2) //ilk denememiz basarisiz oldugunda yeniden deneme sayisi
            .BackOffTime(TimeSpan.FromSeconds(15)) // deneme basarisizliginda bekleme suresi
            .RefreshOnCompleted(true)//tamamlandıktan sonra refresh ile okuma yetkisi verilmis olur. 
            .MaxDegreeOfParallelism(4)
            .Size(100)); //bulk insert boyutu. 100er 100er yapacak. 

            bulkAll.Subscribe(new BulkAllObserver(
                                    onNext: (b) => { Console.WriteLine("aktarım basliyor"); },
                                    onError: (e) => { Console.WriteLine("Hata : {0}", e.Message); },
                                    onCompleted: () => waitHandle.Signal()));
            waitHandle.Wait();
        }
        public static void QuerySample(ElasticClient client)
        {
            var requestContent = new SearchRequest<News>("news-deneme", "news")
            {
                //Term, Prefix vs sorguları da yaratılabilir. Ihtiyaclara gore karar verebilirsiniz. 
                Query = Query<News>.Match(m => m.Field(f => f.NewsTitle).Query("hava")),
            };

            var result = client.Search<News>(requestContent);
            if (result?.Documents != null && result.Documents.Any())
            {
                result.Documents.ToList().ForEach(item => Console.WriteLine("{0} - {1} - {2}", item.NewsId, item.NewsTitle, item.NewsUrl));
            }
        }
    }
}
