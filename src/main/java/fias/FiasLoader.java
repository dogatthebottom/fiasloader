package fias;


import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Date;

import java.util.Properties;

public class FiasLoader {

    public static void main(String [] args) throws ParserConfigurationException, SAXException, IOException, SQLException
    {
        System.out.println("START FIAS LOADING PROCESS=="+new Date().toString());
        if(isDirExist()) {
            loadFiasData();
        }
        System.out.println("STOP FIAS LOADING PROCESS=="+new Date().toString());
    }


    public static XMLReader getXMLReader() throws ParserConfigurationException, SAXException
    {
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        SAXParser saxParser = spf.newSAXParser();
        return saxParser.getXMLReader();
    }

    public static void loadActualStatuses(final XMLReader reader, final Connection connection) throws SQLException, IOException, SAXException
    {
        FiasLoaderHandler handler = new FiasLoaderHandler(
                "ActualStatuses",
                new String [] {"ACTSTATID", "NAME"},
                new String [] {"NUMBER(10)", "NVARCHAR2(100)"},
                connection,
                findDataFile("AS_ACTSTAT")
        );
        reader.setContentHandler(handler);
        reader.parse(handler.getInputSource());
    }

    public static void loadCenterStatuses(final XMLReader reader, final Connection connection) throws SQLException, IOException, SAXException
    {
        FiasLoaderHandler handler = new FiasLoaderHandler(
                "CenterStatuses",
                new String [] {"CENTERSTID", "NAME"},
                new String [] {"NUMBER(10)", "NVARCHAR2(100)"},
                connection,
               findDataFile("AS_CENTERST")
        );
        reader.setContentHandler(handler);
        reader.parse(handler.getInputSource());
    }

    public static void loadCurrentStatuses(final XMLReader reader, final Connection connection) throws SQLException, IOException, SAXException
    {
        FiasLoaderHandler handler = new FiasLoaderHandler(
                "CurrentStatuses",
                new String [] {"CURENTSTID", "NAME"},
                new String [] {"NUMBER(10)", "NVARCHAR2(100)"},
                connection,
                findDataFile("AS_CURENTST")
        );
        reader.setContentHandler(handler);
        reader.parse(handler.getInputSource());
    }

    public static void loadEstateStatuses(final XMLReader reader, final Connection connection) throws SQLException, IOException, SAXException
    {
        FiasLoaderHandler handler = new FiasLoaderHandler(
                "EstateStatuses",
                new String [] {"ESTSTATID", "NAME", "SHORTNAME"},
                new String [] {"NUMBER(10)", "NVARCHAR2(20)", "NVARCHAR2(20)"},
                connection,
                findDataFile("AS_ESTSTAT")
        );
        reader.setContentHandler(handler);
        reader.parse(handler.getInputSource());
    }

    public static void loadHouseStateStatuses(final XMLReader reader, final Connection connection) throws SQLException, IOException, SAXException
    {
        FiasLoaderHandler handler = new FiasLoaderHandler(
                "HouseStateStatuses",
                new String [] {"HOUSESTID", "NAME"},
                new String [] {"NUMBER(10)", "NVARCHAR2(60)"},
                connection,
                 findDataFile("AS_HSTSTAT")
        );
        reader.setContentHandler(handler);
        reader.parse(handler.getInputSource());
    }

    public static void loadIntervalStatuses(final XMLReader reader, final Connection connection) throws SQLException, IOException, SAXException
    {
        FiasLoaderHandler handler = new FiasLoaderHandler(
                "IntervalStatuses",
                new String [] {"INTVSTATID", "NAME"},
                new String [] {"NUMBER(10)", "NVARCHAR2(60)"},
                connection,
                findDataFile("AS_INTVSTAT")
        );
        reader.setContentHandler(handler);
        reader.parse(handler.getInputSource());
    }

    public static void loadNormativeDocumentTypes(final XMLReader xmlReader, final Connection connection) throws SQLException, IOException, SAXException {
        FiasLoaderHandler handler = new FiasLoaderHandler(
                "NormativeDocumentTypes",
                new String [] {"NDTYPEID", "NAME"},
                new String [] {"NUMBER(10)", "NVARCHAR2(250)"},
                connection,
                findDataFile("AS_NDOCTYPE")
        );
        xmlReader.setContentHandler(handler);
        xmlReader.parse(handler.getInputSource());
    }

    public static void loadOperationStatuses(final XMLReader reader, final Connection connection) throws SQLException, IOException, SAXException
    {
        FiasLoaderHandler handler = new FiasLoaderHandler(
                "OperationStatuses",
                new String [] {"OPERSTATID", "NAME"},
                new String [] {"NUMBER(10)", "NVARCHAR2(100)"},
                connection,
                findDataFile("AS_OPERSTAT")
        );
        reader.setContentHandler(handler);
        reader.parse(handler.getInputSource());
    }

    public static void loadStructureStatuses(final XMLReader reader, final Connection connection) throws SQLException, IOException, SAXException
    {
        FiasLoaderHandler handler = new FiasLoaderHandler(
                "StructureStatuses",
                new String [] {"STRSTATID", "NAME", "SHORTNAME"},
                new String [] {"NUMBER(10)", "NVARCHAR2(20)", "NVARCHAR2(20)"},
                connection,
                findDataFile("AS_STRSTAT")
        );
        reader.setContentHandler(handler);
        reader.parse(handler.getInputSource());
    }

    public static void loadAddressObjectTypes(final XMLReader reader, final Connection connection) throws SQLException, IOException, SAXException
    {
        FiasLoaderHandler handler = new FiasLoaderHandler(
                "AddressObjectTypes",
                new String [] {"LLEVEL", "SCNAME", "SOCRNAME", "KOD_T_ST"},
                new String [] {"NUMBER(10)", "NVARCHAR2(10)", "NVARCHAR2(50)", "NVARCHAR2(4)"},
                connection,
                findDataFile("AS_SOCRBASE")
        );
        reader.setContentHandler(handler);
        reader.parse(handler.getInputSource());
    }

    public static void loadSteads(final XMLReader reader, final Connection connection) throws SQLException, IOException, SAXException
    {
        FiasLoaderHandler handler = new FiasLoaderHandler(
                "Steads",
                new String [] {"STEADGUID",     "NNUMBER",        "REGIONCODE",   "POSTALCODE",   "IFNSFL",       "TERRIFNSFL",   "IFNSUL",       "TERRIFNSUL",   "OKATO",         "OKTMO",         "UPDATEDATE", "PARENTGUID",    "STEADID",       "PREVID",        "NEXTID",        "OPERSTATUS", "STARTDATE", "ENDDATE", "NORMDOC",       "LIVESTATUS", "CADNUM",         "DIVTYPE"},
                new String [] {"NVARCHAR2(36)", "NVARCHAR2(120)", "NVARCHAR2(2)", "NVARCHAR2(6)", "NVARCHAR2(4)", "NVARCHAR2(4)", "NVARCHAR2(4)", "NVARCHAR2(4)", "NVARCHAR2(11)", "NVARCHAR2(11)", "DATE",       "NVARCHAR2(36)", "NVARCHAR2(36)", "NVARCHAR2(36)", "NVARCHAR2(36)", "NUMBER(10)", "DATE",      "DATE",    "NVARCHAR2(36)", "NUMBER(1)",  "NVARCHAR2(100)", "NUMBER(1)"},
                connection,
                findDataFile("AS_STEAD")
        );
        reader.setContentHandler(handler);
        reader.parse(handler.getInputSource());
    }

    public static void loadAddressObjects(final XMLReader reader, final Connection connection) throws SQLException, IOException, SAXException
    {
        FiasLoaderHandler handler = new FiasLoaderHandler(
                "AddressObjects",
                new String [] {"AOGUID",        "FORMALNAME",     "REGIONCODE",   "AUTOCODE",     "AREACODE",     "CITYCODE",     "CTARCODE",     "PLACECODE",    "STREETCODE",   "EXTRCODE",     "SEXTCODE",     "OFFNAME",        "POSTALCODE",   "IFNSFL",       "TERRIFNSFL",   "IFNSUL",       "TERRIFNSUL", "OKATO",          "OKTMO",         "UPDATEDATE",  "SHORTNAME",     "AOLEVEL",    "PARENTGUID",    "AOID",          "PREVID",        "NEXTID",        "CODE",          "PLAINCODE",     "ACTSTATUS",  "CENTSTATUS", "OPERSTATUS", "CURRSTATUS", "STARTDATE", "ENDDATE", "NORMDOC",       "LIVESTATUS", "CADNUM",         "DIVTYPE"},
                new String [] {"NVARCHAR2(36)", "NVARCHAR2(120)", "NVARCHAR2(2)", "NVARCHAR2(1)", "NVARCHAR2(3)", "NVARCHAR2(3)", "NVARCHAR2(3)", "NVARCHAR2(3)", "NVARCHAR2(4)", "NVARCHAR2(4)", "NVARCHAR2(3)", "NVARCHAR2(120)", "NVARCHAR2(6)", "NVARCHAR2(4)", "NVARCHAR2(2)", "NVARCHAR2(4)", "NUMBER(4)",  "NVARCHAR2(11)",  "NVARCHAR2(11)", "DATE",        "NVARCHAR2(10)", "NUMBER(10)", "NVARCHAR2(36)", "NVARCHAR2(36)", "NVARCHAR2(36)", "NVARCHAR2(36)", "NVARCHAR2(17)", "NVARCHAR2(15)", "NUMBER(10)", "NUMBER(10)", "NUMBER(10)", "NUMBER(10)", "DATE",      "DATE",    "NVARCHAR2(36)", "NUMBER(1)",  "NVARCHAR2(100)", "NUMBER(1)"},
                connection,
                findDataFile("AS_ADDROBJ")
        );
        reader.setContentHandler(handler);
        reader.parse(handler.getInputSource());
    }

    public static void loadDeletedAddressObjects(final XMLReader reader, final Connection connection) throws SQLException, IOException, SAXException
    {
        FiasLoaderHandler handler = new FiasLoaderHandler(
                "DeletedAddressObjects",
                new String [] {"AOGUID",        "FORMALNAME",     "REGIONCODE",   "AUTOCODE",     "AREACODE",     "CITYCODE",     "CTARCODE",     "PLACECODE",    "STREETCODE",   "EXTRCODE",     "SEXTCODE",     "OFFNAME",        "POSTALCODE",   "IFNSFL",       "TERRIFNSFL",   "IFNSUL",       "TERRIFNSUL", "OKATO",          "OKTMO",         "UPDATEDATE",  "SHORTNAME",     "AOLEVEL",    "PARENTGUID",    "AOID",          "PREVID",        "NEXTID",        "CODE",          "PLAINCODE",     "ACTSTATUS",  "CENTSTATUS", "OPERSTATUS", "CURRSTATUS", "STARTDATE", "ENDDATE", "NORMDOC",       "LIVESTATUS"},
                new String [] {"NVARCHAR2(36)", "NVARCHAR2(120)", "NVARCHAR2(2)", "NVARCHAR2(1)", "NVARCHAR2(3)", "NVARCHAR2(3)", "NVARCHAR2(3)", "NVARCHAR2(3)", "NVARCHAR2(4)", "NVARCHAR2(4)", "NVARCHAR2(3)", "NVARCHAR2(120)", "NVARCHAR2(6)", "NVARCHAR2(4)", "NVARCHAR2(2)", "NVARCHAR2(4)", "NUMBER(4)",  "NVARCHAR2(11)",  "NVARCHAR2(11)", "DATE",        "NVARCHAR2(10)", "NUMBER(10)", "NVARCHAR2(36)", "NVARCHAR2(36)", "NVARCHAR2(36)", "NVARCHAR2(36)", "NVARCHAR2(17)", "NVARCHAR2(15)", "NUMBER(10)", "NUMBER(10)", "NUMBER(10)", "NUMBER(10)", "DATE",      "DATE",    "NVARCHAR2(36)", "NUMBER(1)"},
                connection,
                findDataFile("AS_DEL_ADDROBJ")
        );
        reader.setContentHandler(handler);
        reader.parse(handler.getInputSource());
    }

    public static void loadRooms(final XMLReader reader, final Connection connection) throws SQLException, IOException, SAXException
    {
        FiasLoaderHandler handler = new FiasLoaderHandler(
                "Rooms",
                new String [] {"ROOMGUID",      "FLATNUMBER",    "FLATTYPE",  "ROOMNUMBER",    "ROOMTYPE",  "REGIONCODE",   "POSTALCODE",   "UPDATEDATE",  "HOUSEGUID",     "ROOMID",        "PREVID",        "NEXTID",        "STARTDATE", "ENDDATE", "LIVESTATUS", "NORMDOC"      , "OPERSTATUS", "CADNUM",         "ROOMCADNUM"},
                new String [] {"NVARCHAR2(36)", "NVARCHAR2(50)", "NUMBER(1)", "NVARCHAR2(50)", "NUMBER(1)", "NVARCHAR2(2)", "NVARCHAR2(6)", "DATE",        "NVARCHAR2(36)", "NVARCHAR2(36)", "NVARCHAR2(36)", "NVARCHAR2(36)", "DATE",      "DATE",    "NUMBER(1)",  "NVARCHAR2(36)", "NUMBER(10)", "NVARCHAR2(100)", "NVARCHAR2(100)"},
                connection,
                findDataFile("AS_ROOM")
        );
        reader.setContentHandler(handler);
        reader.parse(handler.getInputSource());
    }
    public static void loadHouses(final XMLReader reader, final Connection connection) throws SQLException, IOException, SAXException
    {
        FiasLoaderHandler handler = new FiasLoaderHandler(
                "Houses",
                new String [] {"POSTALCODE",   "REGIONCODE",   "IFNSFL",       "TERRIFNSFL",   "IFNSUL",       "TERRIFNSUL",   "OKATO",         "OKTMO",         "UPDATEDATE", "HOUSENUM"      , "ESTSTATUS", "BUILDNUM",      "STRUCNUM",      "STRSTATUS",  "HOUSEID",       "HOUSEGUID",     "AOGUID",        "STARTDATE", "ENDDATE", "STATSTATUS", "NORMDOC",       "COUNTER",    "CADNUM",         "DIVTYPE"},
                new String [] {"NVARCHAR2(6)", "NVARCHAR2(2)", "NVARCHAR2(4)", "NVARCHAR2(4)", "NVARCHAR2(4)", "NVARCHAR2(4)", "NVARCHAR2(11)", "NVARCHAR2(11)", "DATE",       "NVARCHAR2(20)",  "NUMBER(1)", "NVARCHAR2(10)", "NVARCHAR2(10)", "NUMBER(10)", "NVARCHAR2(36)", "NVARCHAR2(36)", "NVARCHAR2(36)", "DATE",      "DATE",    "NUMBER(10)", "NVARCHAR2(36)", "NUMBER(10)", "NVARCHAR2(100)", "NUMBER(1)"},
                connection,
               findDataFile("AS_HOUSE")
        );
        reader.setContentHandler(handler);
        reader.parse(handler.getInputSource());
    }

    private static Connection getConnection() throws IOException {

        Properties prop = getProperties();
        String db_host = prop.getProperty("DB_HOST").toString();
        String db_port = prop.getProperty("DB_PORT").toString();
        String db_sid =prop.getProperty("DB_SID");
        String db_user = prop.getProperty("DB_USER").toString();
        String db_pwd = prop.getProperty("DB_PASSWORD").toString();

        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection connection = null;

        try {
            connection = DriverManager.getConnection(
                    "jdbc:oracle:thin:@"+db_host+":"+db_port+":"+db_sid, db_user, db_pwd);
        } catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
        }
         return connection;
    }

    private static String findDataFile(final String fType) throws IOException {
        Properties prop = getProperties();
        String filesPath=prop.getProperty("ROOT_PATH").toString();
        File dir = new File(filesPath+"/fias_xml");
        File[] matches = dir.listFiles(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                try {
                    return name.startsWith(fType) && name.endsWith(".XML");
                }catch (Exception e){
                    e.printStackTrace();
                    System.out.println("Files not found");
                    return false;
                }
            }
        });
        String xmlfile="";
          try {
              xmlfile=matches[0].getAbsolutePath();
          }catch(Exception e){
              xmlfile="";
              System.out.println("File not found");
          }
        System.out.println("xml file=="+xmlfile);
        return xmlfile;
    }

    private  static void truncateTable(String tableName,Connection con){
        Statement stTruncate = null;
        try {
            stTruncate = con.createStatement();
            stTruncate.executeUpdate("truncate table "+tableName);
            stTruncate.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    private static void loadFiasData() throws ParserConfigurationException, SAXException, SQLException, IOException {
        XMLReader xmlReader = getXMLReader();
        Connection connection = getConnection();

        //Загрузка ACTUALSTATUSES
        truncateTable("ACTUALSTATUSES",connection);
        loadActualStatuses(xmlReader, connection);

        //Загрузка CENTERSTATUSES
        truncateTable("CENTERSTATUSES",connection);
        loadCenterStatuses(xmlReader, connection);

        //Загрузка CURRENTSTATUSES
        truncateTable("CURRENTSTATUSES",connection);
        loadCurrentStatuses(xmlReader, connection);

        //Загрузка ESTATESTATUSES
        truncateTable("ESTATESTATUSES",connection);
        loadEstateStatuses(xmlReader, connection);

        //Загрузка HOUSESTATESTATUSES
        truncateTable("HOUSESTATESTATUSES",connection);
        loadHouseStateStatuses(xmlReader, connection);

        //Загрузка INTERVALSTATUSES
        truncateTable("INTERVALSTATUSES",connection);
        loadIntervalStatuses(xmlReader, connection);

        //Загрузка NORMATIVEDOCUMENTTYPES
        truncateTable("NORMATIVEDOCUMENTTYPES",connection);
        loadNormativeDocumentTypes(xmlReader, connection);

        //Загрузка OPERATIONSTATUSES
        truncateTable("OPERATIONSTATUSES",connection);
        loadOperationStatuses(xmlReader, connection);

        //загрузка STRUCTURESTATUSES
        truncateTable("STRUCTURESTATUSES",connection);
        loadStructureStatuses(xmlReader, connection);

        //Загрузка ADDRESSOBJECTTYPES
        truncateTable("ADDRESSOBJECTTYPES",connection);
        loadAddressObjectTypes(xmlReader, connection);

        //Загрузка STEADS
        truncateTable("STEADS",connection);
        loadSteads(xmlReader, connection);

        //Загрузка ADDRESSOBJECTS
        //dropIndex("IDX_STATUS_AOGUID",connection);
        dropIndex("IDX_STATUS_PARENT_LEVEL",connection);
       // dropIndex("IDX_STATUS_PARENT_LEVEL_DATES",connection);
        truncateTable("ADDRESSOBJECTS",connection);
        loadAddressObjects(xmlReader,connection);
        createIndexAdressObjecs(connection);

        //Загрузка DELETEDADDRESSOBJECTS
        truncateTable("DELETEDADDRESSOBJECTS",connection);
        loadDeletedAddressObjects(xmlReader, connection);

        //Загрузка HOUSES
        dropIndex("IDX_AOGUID",connection);
        dropIndex("IDX_START_END_AUGUID",connection);
        dropIndex("IDX_START_END_HOUSEGUID",connection);
        truncateTable("HOUSES",connection);
        loadHouses(xmlReader, connection);
        createIndexHouses(connection);

        connection.close();
    }

    private static Properties getProperties() throws IOException {
        Properties props = new Properties();
        props.load(new FileInputStream(new File("loadproperties.ini")));
        return props;
    }

    private  static void dropIndex(String indexName,Connection con){
        Statement st = null;
        try {
            st = con.createStatement();
            st.executeUpdate("DROP INDEX "+indexName);
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createIndexAdressObjecs(Connection con){
        System.out.println("Creating indexes for AddressObject");
        Statement st = null;
        try {
            st = con.createStatement();
            st.executeUpdate("CREATE INDEX IDX_STATUS_PARENT_LEVEL ON ADDRESSOBJECTS (LIVESTATUS, PARENTGUID, AOLEVEL)");
            System.out.println("Index for AddressObject IDX_STATUS_PARENT_LEVEL has been created");
            //st.executeUpdate("CREATE INDEX IDX_STATUS_AOGUID ON ADDRESSOBJECTS (AOGUID, LIVESTATUS)");
           // System.out.println("Index for AddressObject IDX_STATUS_AOGUID has been created");
           // st.executeUpdate("CREATE INDEX IDX_STATUS_PARENT_LEVEL_DATES ON ADDRESSOBJECTS (PARENTGUID, LIVESTATUS, AOLEVEL, STARTDATE, ENDDATE) ");
           // System.out.println("Index for AddressObject IDX_STATUS_PARENT_LEVEL_DATES has been created");
            System.out.println("Indexes for AddressObject have been created");
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createIndexHouses(Connection con){
        System.out.println("Creating indexes for Houses");
        Statement st = null;
        try {
            st = con.createStatement();
            st.executeUpdate("CREATE INDEX IDX_AOGUID ON HOUSES (AOGUID)");
            System.out.println("Index for Houses IDX_AOGUID has been created");
            st.executeUpdate("CREATE INDEX IDX_START_END_AUGUID ON HOUSES (STARTDATE, ENDDATE, AOGUID)");
            System.out.println("Index for Houses IDX_START_END_AUGUID has been created");
            st.executeUpdate("CREATE INDEX IDX_START_END_HOUSEGUID ON HOUSES (HOUSEGUID, STARTDATE, ENDDATE)");
            System.out.println("Index for Houses IDX_START_END_HOUSEGUID has been created");
            System.out.println("Indexes for Houses have been created");
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
    private static boolean isDirExist() throws IOException {
        Properties prop = getProperties();
        String filesPath=prop.getProperty("ROOT_PATH").toString();
        File dir = new File(filesPath+"/fias_xml");
        if (dir.exists()){
            return true;
        }
        return false;
    }
}
