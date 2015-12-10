package xml.parsing;

import java.util.ArrayList;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class Functions {

	static final Logger log = LogManager.getLogger(Functions.class);
	int numberoftablecolumns;
	ArrayList<String> tablecolumns;
	ArrayList<String> attributesprimarykey;
	String[][] attributesextra;
	ArrayList<String> foreignkeyname;
	ArrayList<String> uniquekeyname;
	private DBOperations db;

	public Functions(DBOperations db) {
		this.db = db;
	}

	public void CreateTable(NodeList nodelisttableengineandcollation,
			NodeList nodelisttablecolumns, String tablename) {
		Node nodetableengineandcollation = nodelisttableengineandcollation
				.item(0);
		Element elementtableengine = (Element) nodetableengineandcollation;
		String tableengine = elementtableengine.getAttribute("Engine");

		Element elementtablecollation = (Element) nodetableengineandcollation;
		String tablecollation = elementtablecollation.getAttribute("Collation");

		log.info("Table name: " + tablename);

		numberoftablecolumns = nodelisttablecolumns.getLength();
		tablecolumns = new ArrayList<>();
		attributesprimarykey = new ArrayList<>();
		attributesextra = new String[numberoftablecolumns][2];

		for (int temp = 0; temp < numberoftablecolumns; temp++) {

			Node nodetablecolumns = nodelisttablecolumns.item(temp);
			Element tablecolumn = (Element) nodetablecolumns;

			String field = tablecolumn.getAttribute("Field");
			log.info("Field name: " + field);
			tablecolumns.add(field);
			String type = tablecolumn.getAttribute("Type");

			if (temp == 0) {
				String createtable = "CREATE TABLE " + tablename + " (" + field
						+ " " + type + ") ENGINE = " + tableengine
						+ " COLLATE " + tablecollation;
				;
				db.Executions(createtable);
			}

			else {
				String altertable = "ALTER TABLE " + tablename + " ADD "
						+ field + " " + type;
				db.Executions(altertable);
			}

			Node nodetablecolumnsprimarykey = nodelisttablecolumns.item(temp);
			Element elementtablecolumnsprimarykey = (Element) nodetablecolumnsprimarykey;
			String primarykeyexist = elementtablecolumnsprimarykey
					.getAttribute("Key");

			if (primarykeyexist.equals("PRI")) {
				log.info(field + " is primary key");
				attributesprimarykey.add(field);
			}

			Node nodetablecolumnsextra = nodelisttablecolumns.item(temp);
			Element elementtableattributeextra = (Element) nodetablecolumnsextra;
			String extraexist = elementtableattributeextra
					.getAttribute("Extra");

			if (extraexist.equals("auto_increment")) {
				log.info(field + " has auto increment");
				attributesextra[temp][0] = field;
				attributesextra[temp][1] = type;
			}
		}

		if (!attributesprimarykey.isEmpty()) {
			String sql = "";
			int numberofprimarykeys = attributesprimarykey.size();

			if (numberofprimarykeys == 1) {
				sql = "ALTER TABLE " + tablename
						+ " ADD CONSTRAINT PRIMARY KEY ("
						+ attributesprimarykey.get(0) + ")";
			} else {
				for (int temp = 0; temp < numberofprimarykeys; temp++) {
					if (temp == 0) {
						sql = "ALTER TABLE " + tablename
								+ " ADD CONSTRAINT PRIMARY KEY ("
								+ attributesprimarykey.get(temp);
					} else if (temp == numberofprimarykeys - 1) {
						sql = sql + "," + attributesprimarykey.get(temp) + ")";
					} else {
						sql = sql + "," + attributesprimarykey.get(temp);
					}
				}
			}
			db.Executions(sql);
		}

		for (int temp = 0; temp < numberoftablecolumns; temp++) {
			if (attributesextra[temp][0] != null
					&& attributesextra[temp][1] != null) {
				String sql = "";
				sql = "ALTER TABLE " + tablename + " MODIFY "
						+ attributesextra[temp][0] + " "
						+ attributesextra[temp][1] + " AUTO_INCREMENT";
				db.Executions(sql);
			}

		}
	}

	public void InsertData(NodeList nodetablerow, String tablename) {
		int numberofrows = nodetablerow.getLength();

		log.info("Inserting data inside the tables");
		for (int temp = 0; temp < numberofrows; temp++) {
			Node noderow = nodetablerow.item(temp);
			Element tableelement = (Element) noderow;
			String sql = "";

			if (numberoftablecolumns == 1) {
				sql = "INSERT INTO " + tablename + " (" + tablecolumns.get(0)
						+ ") VALUES (";
			} else {
				for (int z = 0; z < numberoftablecolumns; z++) {
					if (z == 0) {
						sql = "INSERT INTO " + tablename + " ("
								+ tablecolumns.get(z);
					} else if (z == numberoftablecolumns - 1) {
						sql = sql + "," + tablecolumns.get(z) + ") VALUES (";
					} else if (z > 0 && z < numberoftablecolumns - 1) {
						sql = sql + "," + tablecolumns.get(z);
					}
				}
			}
			for (int y = 0; y < numberoftablecolumns; y++) {
				String content = tableelement.getElementsByTagName("field")
						.item(y).getTextContent();

				if (y == numberoftablecolumns - 1)
					sql = sql + "'" + content + "')";
				else
					sql = sql + "'" + content + "', ";
			}
			db.Executions(sql);
		}
	}

	public void InsertTrigger(NodeList nodelisttabletriggers) {
		int numberoftabletriggers = nodelisttabletriggers.getLength();

		for (int z = 0; z < numberoftabletriggers; z++) {
			Node nodetabletriggers = nodelisttabletriggers.item(z);
			Element elementtabletriggers = (Element) nodetabletriggers;
			String tabletrigger = elementtabletriggers.getTextContent();
			String sql = "CREATE ";
			int index = tabletrigger.indexOf("TRIGGER");
			log.info(tabletrigger);
			tabletrigger = tabletrigger.substring(index);
			sql = sql.concat(tabletrigger) + ";";
			db.Executions(sql);
		}
	}

	public void FindForeignAndUniqueKey(NodeList nodelistforeignandunique,
			String databasename) {
		int lengthofrows = nodelistforeignandunique.getLength();
		foreignkeyname = new ArrayList<>();
		uniquekeyname = new ArrayList<>();

		for (int temp = 0; temp < lengthofrows; temp++) {

			Node nodetableforeignandunique = nodelistforeignandunique
					.item(temp);
			Element elementconstraintsname = (Element) nodetableforeignandunique;
			String constraintschema = elementconstraintsname
					.getElementsByTagName("field").item(1).getTextContent();
			if (constraintschema.equals(databasename)) {
				String constrainttype = elementconstraintsname
						.getElementsByTagName("field").item(5).getTextContent();
				if (constrainttype.equals("FOREIGN KEY")
						|| constrainttype.equals("FOREIGN")) {
					String constraintname = elementconstraintsname
							.getElementsByTagName("field").item(2)
							.getTextContent();
					log.info("Foreign key name: " + constraintname);
					foreignkeyname.add(constraintname);
				}
				if (constrainttype.equals("UNIQUE KEY")
						|| constrainttype.equals("UNIQUE")) {
					String constraintname = elementconstraintsname
							.getElementsByTagName("field").item(2)
							.getTextContent();
					String constrainttablename = elementconstraintsname
							.getElementsByTagName("field").item(4)
							.getTextContent();
					log.info("Unique key name: " + constraintname
							+ " and its table name: " + constrainttablename);
					uniquekeyname.add(constraintname);
					uniquekeyname.add(constrainttablename);
				}
			}
		}
	}

	public void InsertForeignAndUniqueKey(
			NodeList nodelistforeignanduniqueinsert, String databasename) {
		int lengthofforeignanduniqueinsert = nodelistforeignanduniqueinsert
				.getLength();
		int numberofforeignkey = foreignkeyname.size();
		int numberofuniquekey = uniquekeyname.size();

		for (int y = 0; y < numberofuniquekey; y = y + 2) {
			String uniquekey = uniquekeyname.get(y);
			String uniquekeytablename = uniquekeyname.get(y + 1);
			int numberofkeys = 0;
			String sql = "";
			for (int x = 0; x < lengthofforeignanduniqueinsert; x++) {
				Node nodetableforeignanduniqueinsert = nodelistforeignanduniqueinsert
						.item(x);
				Element elementconstraintsnameinsert = (Element) nodetableforeignanduniqueinsert;
				String constraintschema = elementconstraintsnameinsert
						.getElementsByTagName("field").item(1).getTextContent();
				if (constraintschema.equals(databasename)) {
					String constraintname = elementconstraintsnameinsert
							.getElementsByTagName("field").item(2)
							.getTextContent();
					if (uniquekey.equals(constraintname)) {
						String constrainttablename = elementconstraintsnameinsert
								.getElementsByTagName("field").item(5)
								.getTextContent();
						if (uniquekeytablename.equals(constrainttablename)) {
							String constraintcolumnname = elementconstraintsnameinsert
									.getElementsByTagName("field").item(6)
									.getTextContent();

							if (numberofkeys == 0) {
								sql = "ALTER TABLE " + constrainttablename
										+ " ADD CONSTRAINT UNIQUE ("
										+ constraintcolumnname;
								numberofkeys++;
							} else {
								sql = sql + "," + constraintcolumnname;
							}

						}
					}
				}
			}
			sql = sql + ")";
			db.Executions(sql);
		}

		for (int x = 0; x < numberofforeignkey; x++) {
			String foreignkey = foreignkeyname.get(x);
			String sql = "";
			for (int y = 0; y < lengthofforeignanduniqueinsert; y++) {
				Node nodetableforeignanduniqueinsert = nodelistforeignanduniqueinsert
						.item(y);
				Element elementconstraintsnameinsert = (Element) nodetableforeignanduniqueinsert;
				String constraintschema = elementconstraintsnameinsert
						.getElementsByTagName("field").item(1).getTextContent();
				if (constraintschema.equals(databasename)) {
					String constraintname = elementconstraintsnameinsert
							.getElementsByTagName("field").item(2)
							.getTextContent();
					if (foreignkey.equals(constraintname)) {
						String constrainttablename = elementconstraintsnameinsert
								.getElementsByTagName("field").item(5)
								.getTextContent();
						String constraintcolumnname = elementconstraintsnameinsert
								.getElementsByTagName("field").item(6)
								.getTextContent();
						String constraintreferencedtablename = elementconstraintsnameinsert
								.getElementsByTagName("field").item(10)
								.getTextContent();
						String constraintreferencedcolumnname = elementconstraintsnameinsert
								.getElementsByTagName("field").item(11)
								.getTextContent();
						sql = "ALTER TABLE " + constrainttablename
								+ " ADD CONSTRAINT FOREIGN KEY ("
								+ constraintcolumnname + ") REFERENCES "
								+ constraintreferencedtablename + "("
								+ constraintreferencedcolumnname + ")";
					}
				}
			}
			db.Executions(sql);
		}
	}
}
