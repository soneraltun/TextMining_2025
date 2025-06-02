package org.example;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.example.LuceneUrlIndexer.indexUrlsFromBz2File;
import static org.example.LuceneUrlIndexer.search;

public class Main{
    public static void main(String[] args) {
        try {
            String bz2Path = "ClueWeb12_All_edocid2url.txt.bz2";
            indexUrlsFromBz2File(bz2Path);

            // Tüm topic dosyalarını sırayla oku
            Map<String, String> allQueries = new LinkedHashMap<>();
            allQueries.putAll(loadQueriesFromFile("web2013.topics.txt"));
            allQueries.putAll(loadQueriesFromFile("web2014.topics.txt"));

            // Sonuç dosyasını hazırla
            BufferedWriter writer = new BufferedWriter(new FileWriter("results.txt"));
            for (Map.Entry<String, String> entry : allQueries.entrySet()) {
                String qid = entry.getKey();
                String queryText = entry.getValue();

                List<SearchResult> topDocs = searchWithLucene(queryText, 1000);
                int rank = 1;
                for (SearchResult result : topDocs) {
                    writer.write(String.format("%s\tQ0\t%s\t%d\t%.6f\tLetter_Tokenizer\n",
                            qid, result.docId, rank, result.score));

                    rank++;
                }
            }
            writer.close();
            System.out.println("results.txt dosyası oluşturuldu.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Map<String, String> loadQueriesFromFile(String path) throws IOException {
        Map<String, String> qidToQuery = new LinkedHashMap<>();
        BufferedReader reader = new BufferedReader(new FileReader(path));
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.contains(":")) {
                String[] parts = line.split(":", 2);
                qidToQuery.put(parts[0].trim(), parts[1].trim());
            }
        }
        reader.close();
        return qidToQuery;
    }

    public static List<SearchResult> searchWithLucene(String queryStr, int maxHits) throws Exception {
        List<SearchResult> resultsList = new ArrayList<>();
        Directory dir = FSDirectory.open(Paths.get("lucene_index"));
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);
        Analyzer analyzer = new StandardAnalyzer();
        QueryParser parser = new QueryParser("content", analyzer);
        Query query = parser.parse(QueryParser.escape(queryStr));

        TopDocs results = searcher.search(query, maxHits);
        for (ScoreDoc hit : results.scoreDocs) {
            Document doc = searcher.doc(hit.doc);
            resultsList.add(new SearchResult(doc.get("docid"), hit.score));
        }
        reader.close();
        return resultsList;
    }

    public static class SearchResult {
        String docId;
        float score;
        public SearchResult(String docId, float score) {
            this.docId = docId;
            this.score = score;
        }
    }

}
