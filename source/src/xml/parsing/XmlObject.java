package xml.parsing;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.*;

public class XmlObject {

	static final Logger log = LogManager.getLogger(XmlObject.class);

	public static void main(String argv[]) {
		try {

			DBOperations db = new DBOperations();
			Functions func = new Functions(db);

			String hostname = argv[0];
			String username = argv[1];
			String psswd = argv[2];
			File xmlfile = new File(argv[3]);
			String dbname = argv[4];
			DocumentBuilderFactory dbfactory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder dbuilder = dbfactory.newDocumentBuilder();
			Document doc = dbuilder.parse(xmlfile);
			doc.getDocumentElement().normalize();

			XPath XPath = XPathFactory.newInstance().newXPath();
			log.info("Started to read XML file");

			String databasename = "/mysqldump/database";
			NodeList nodelistdatabasename = (NodeList) XPath.compile(
					databasename).evaluate(doc, XPathConstants.NODESET);
			Node nodedatabasename = nodelistdatabasename.item(0);
			Element elementdatabasename = (Element) nodedatabasename;
			databasename = elementdatabasename.getAttribute("name");

			db.setHostName(hostname);
			db.setUser(username);
			db.setPassword(psswd);
			if (dbname.equals("default")) {
				db.setDBName(databasename);
			} else {
				db.setDBName(dbname);
			}
			log.info("Trying to connect");
			db.DBConnection();
			log.info("Connected to the " + username);
			db.CreateDatabase();

			String tables = "/mysqldump/database[@name='" + databasename
					+ "']/table_structure";
			NodeList nodelisttables = (NodeList) XPath.compile(tables)
					.evaluate(doc, XPathConstants.NODESET);
			int numberoftables = nodelisttables.getLength();
			log.info("Creating database tables");
			for (int x = 0; x < numberoftables; x++) {
				log.info("------------------------");
				Node tablenode = nodelisttables.item(x);
				Element tableelements = (Element) tablenode;
				String tablename = tableelements.getAttribute("name");

				String tableengineandcollation = "/mysqldump/database[@name='"
						+ databasename + "']/table_structure[@name='"
						+ tablename + "']/options";
				NodeList nodelisttableengineandcollation = (NodeList) XPath
						.compile(tableengineandcollation).evaluate(doc,
								XPathConstants.NODESET);

				String tablefield = "/mysqldump/database[@name='"
						+ databasename + "']/table_structure[@name='"
						+ tablename + "']/field";
				NodeList nodelisttableattributes = (NodeList) XPath.compile(
						tablefield).evaluate(doc, XPathConstants.NODESET);

				func.CreateTable(nodelisttableengineandcollation,
						nodelisttableattributes, tablename);

				String tabletriggers = "/mysqldump/database[@name='"
						+ databasename + "']/triggers[@name='" + tablename
						+ "']/trigger";

				NodeList nodelisttabletriggers = (NodeList) XPath.compile(
						tabletriggers).evaluate(doc, XPathConstants.NODESET);

				func.InsertTrigger(nodelisttabletriggers);

				String tablerow = "/mysqldump/database[@name='" + databasename
						+ "']/table_data[@name='" + tablename + "']/row";
				NodeList nodetablerow = (NodeList) XPath.compile(tablerow)
						.evaluate(doc, XPathConstants.NODESET);

				func.InsertData(nodetablerow, tablename);
			}

			String tableforeignandunique = "/mysqldump/database[@name='INFORMATION_SCHEMA']/table_data[@name='TABLE_CONSTRAINTS']/row";
			NodeList nodelistforeignandunique = (NodeList) XPath.compile(
					tableforeignandunique)
					.evaluate(doc, XPathConstants.NODESET);
			log.info("------------------------");
			func.FindForeignAndUniqueKey(nodelistforeignandunique, databasename);

			String tableforeignanduniqueinsert = "/mysqldump/database[@name='INFORMATION_SCHEMA']/table_data[@name='KEY_COLUMN_USAGE']/row";
			NodeList nodelistforeignanduniqueinsert = (NodeList) XPath.compile(
					tableforeignanduniqueinsert).evaluate(doc,
					XPathConstants.NODESET);

			func.InsertForeignAndUniqueKey(nodelistforeignanduniqueinsert,
					databasename);

			log.info("------------------------");
			if (dbname.equals("default")) {
				log.info(databasename + " database created.");
			} else {
				log.info(dbname + " database created.");
			}

		} catch (Exception e) {
			log.error("No such file or directory");
			log.debug(e, e);
			System.exit(1);
		}
	}
}
