package rdfparsercheckuoa.rdf_parser_check;

import org.apache.spark.sql.Row;

import java.util.List;

public class URITable {
    private String full;
    private String left;
    private String right;

    public String getFull() {
        return full;
    }

    public String getLeft() {
        return left;
    }

    public String getRight() {
        return right;
    }



    public URITable(Row item) {
        full = item.getString(0);
        List<String> l = App.splitURI(item);
        left = App.reconURI(l, 1, 6 , "https:/");
        right = App.reconURI(l, 7, -1, "");



    }
}
