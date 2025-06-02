package org.example;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.index.IndexWriterConfig;

import java.io.*;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

public class LuceneUrlIndexer {

    private static final String INDEX_DIR = "lucene_index";

    public static void indexUrlsFromBz2File(String bz2FilePath) throws IOException {
        Directory dir = FSDirectory.open(Paths.get(INDEX_DIR));
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(dir, config);

        try (
                FileInputStream fin = new FileInputStream(bz2FilePath);
                BufferedInputStream bis = new BufferedInputStream(fin);
                BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(bis);
                BufferedReader reader = new BufferedReader(new InputStreamReader(bzIn, "UTF-8"))
        ) {
            String line;
            int count = 0;
            Set<String> addedDocIds = new HashSet<>();
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ", 2);
                if (parts.length < 2) continue;

                String docId = parts[0];
                String url = parts[1];

                if (addedDocIds.contains(docId)) continue;
                addedDocIds.add(docId);

                Document doc = new Document();
                doc.add(new StringField("docid", docId, Field.Store.NO));
                doc.add(new StoredField("url", url));
                doc.add(new TextField("content", url, Field.Store.NO));

                writer.addDocument(doc);
                count++;

                if (count % 100000 == 0) {
                    writer.commit();
                    System.gc();
                    System.out.println(count + " belge eklendi...");
                }
            }

        }

        writer.close();
        System.out.println("BZ2 dosyasından indeksleme tamamlandı.");
    }

    public static void search(String queryStr) throws Exception {
        Directory dir = FSDirectory.open(Paths.get(INDEX_DIR));
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);
        Analyzer analyzer = new StandardAnalyzer();

        QueryParser parser = new QueryParser("content", analyzer);
        Query query = parser.parse(queryStr);

        TopDocs results = searcher.search(query, 10);
        for (ScoreDoc hit : results.scoreDocs) {
            Document doc = searcher.doc(hit.doc);
            System.out.println(hit.score + " - " + doc.get("docid") + " → " + doc.get("url"));
        }

        reader.close();
    }
}
