package fias;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Map;

public class FiasLoaderHandler extends DefaultHandler {
    private static final boolean DEBUG = true;
    private static final int SIZE_OF_BATCH = 5000;
    private static final String DATE = "DATE";
    private static final String NVARCHAR = "NVARCHAR";
    private static final String NUMBER = "NUMBER";

    private static Map<String, TypeMarchaller> factory = new Hashtable<>();
    static {
        factory.put(DATE, new DateTypeMarshaller());
        factory.put(NVARCHAR, new StringTypeMarshaller());
        factory.put(NUMBER, new NumberTypeMarshaller());
    }

    private final String tableName;
    private final String [] columnNames;
    private final String [] columnTypes;
    private final Connection connection;
    private final int numberOfColumns;
    private final String filePath;
    private final PreparedStatement statement;

    private int statementsProcessed = 0;

    public FiasLoaderHandler(
            final String tableName,
            final String [] columnNames,
            final String [] columnTypes,
            final Connection connection,
            final String filePath
    ) throws SQLException
    {
        // TODO throw exceptions on null arguments, zero size arguments etc
        this.tableName = tableName;
        this.columnNames = columnNames;
        numberOfColumns = columnNames.length;
        this.columnTypes = columnTypes;
        this.connection = connection;
        if(!connection.getAutoCommit())
            this.connection.setAutoCommit(true);
        statement = prepareStatement();
        this.filePath = filePath;
    }

    public InputSource getInputSource() throws IOException
    {
        Path p = Paths.get(filePath);
        InputStream in = Files.newInputStream(p);
        return new InputSource(in);
    }

    private String getInsertStatement()
    {
        StringBuilder builder = new StringBuilder("insert into ");
        builder.append(tableName);
        builder.append(" values ( ");
        for(int i = 0; i < numberOfColumns; i++)
        {
            if(i != 0)
                builder.append(" , ");

            builder.append(factory.get(getType(i)).getStringForPreparedStatement());
        }
        builder.append(" ) ");
        return builder.toString();
    }

    private String getType(final int i)
    {
        String type = columnTypes[i].toUpperCase().trim();
        if(type.startsWith(DATE))
            return DATE;
        else if(type.startsWith(NUMBER))
            return NUMBER;
        else
            return NVARCHAR; // TODO Other types
    }

    private PreparedStatement prepareStatement() throws SQLException {
        return connection.prepareStatement(getInsertStatement());
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        try {
            for(int i = 0; i < numberOfColumns; i++)
            {
                setStatementAttribute(i, attributes);
            }
            statement.addBatch();
            statementsProcessed++;
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void setStatementAttribute(int index, Attributes attrs) throws SQLException {
        String name = columnNames[index].toUpperCase();
        String type = getType(index);
        String value = attrs.getValue(name);

        if(value == null)
            statement.setNull( (index + 1), factory.get(type).getNullSqlType());
        else
            factory.get(type).setStatementAttributeValue(statement, index, value);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if(statementsProcessed > 0 && statementsProcessed % SIZE_OF_BATCH == 0)
            applyBatch();
    }

    private void applyBatch() {
        try {
            statement.executeBatch();
            if(DEBUG)
                System.out.println("Statements processed so far: "+ statementsProcessed);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void endDocument() throws SAXException {
        applyBatch();
    }

    private static interface TypeMarchaller
    {
        public int getNullSqlType();
        public void setStatementAttributeValue(final PreparedStatement s, final int i, final String v)  throws NumberFormatException, SQLException;
        public String getStringForPreparedStatement();
    }

    private static class NumberTypeMarshaller implements TypeMarchaller
    {
        @Override
        public int getNullSqlType() {
            return java.sql.Types.NUMERIC;
        }

        @Override
        public void setStatementAttributeValue(final PreparedStatement s, final int i, final String v) throws NumberFormatException, SQLException {
            s.setInt( (i+1), Integer.parseInt(v));
        }

        @Override
        public String getStringForPreparedStatement() {
            return " ? ";
        }
    }

    private static class DateTypeMarshaller extends StringTypeMarshaller
    {
        @Override
        public int getNullSqlType() {
            return java.sql.Types.DATE;
        }

        @Override
        public String getStringForPreparedStatement() {
            return " TO_DATE(?, 'YYYY-MM-DD') ";
        }
    }

    private static class StringTypeMarshaller extends NumberTypeMarshaller
    {
        @Override
        public int getNullSqlType() {
            return java.sql.Types.NVARCHAR;
        }

        @Override
        public void setStatementAttributeValue(PreparedStatement s, int i, String v) throws NumberFormatException, SQLException {
            s.setString((i + 1), v);
        }
    }
}
