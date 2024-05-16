using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Data2Checker
{
    class SQLMethods
    {
        static OdbcConnection OdbcSDL = new OdbcConnection("DSN=Parity_SDL;Pooling=true;");
        static OdbcConnection OdbcHBS = new OdbcConnection("DSN=Parity_HBS;Pooling=true;");
        public OdbcConnection[] Connections = new OdbcConnection[] { OdbcSDL, OdbcHBS };
        public DataTableCollection TableCollection { get; set; }
        static Operations Operations = new Operations();
        static DataTable Atradius = new DataTable();
        public string ConnString { get; set; }
        public static List<DataTable> Tables { get; set; }
        public List<string> Queries { get; set; }
        public DataTable LieferantenASCII = new DataTable();
        public static DataTable dataTable = new DataTable();
        static string Datum = string.Empty;
        static string DateFile = @"\\bauer-gmbh.org\DFS\SL\PROALPHA\10. Team-DÜ, EDI, WF\Datenexport\KundenLieferanten\LastDate.txt";
        public SQLMethods()
        {
            Operations = new Operations();
            Operations.FillAtradius(Atradius);

        }

        public DataTable GetTable(string query, OdbcConnection connection, string tableName)
        {
            DataTable table = new DataTable(tableName);

            using (OdbcCommand command = new OdbcCommand(query, connection))
            {
                using (OdbcDataReader reader = command.ExecuteReader())
                {
                    table.Load(command.ExecuteReader());
                }
            }

            return table;
        }

        public  string GetStandort(OdbcConnection connection)
        {
            string standort = string.Empty;

            if (connection.ConnectionString.Contains("SDL"))
            {
                standort = "1";
            }

            else if (connection.ConnectionString.Contains("HBS"))
            {
                standort = "2";
            }

            return standort;
        }

        static void SetDataTables(List<DataTable> tables, OdbcConnection connection)
        {
            SQLMethods methods = new SQLMethods();
            OdbcConnection conn = new OdbcConnection(connection.ConnectionString);

            if (connection.ConnectionString.Contains("SDL"))
            {
                tables.Add(methods.GetKundenASCII(connection, standort: "1"));

                tables.Add(methods.GetBelegeASCII(connection));
            }
            else if (connection.ConnectionString.Contains("SDL"))
            {
            }
        }

        //SQL-Queries SDL
        public Dictionary<string, string> QueriesSDL = new Dictionary<string, string>()
        {

            {"DU_Kunde", "SELECT " +
                   "kunden.kdn_kontonr," +
                   "'', " +
                   "anschrift.name_001, " +
                   "anschrift.land, " +
                   "anschrift.ort, " +
                   "anschrift.anssuch, " +
                   "anschrift.ans_suwo2, " +
                   "'@', " +
                   "'@'," +
                   "'@', " +                              //09
                   "'@', " +
                   "anschrift.name_002," +                //11
                   "anschrift.name_003, " +
                   "anschrift.plz," +
                   " '@' AS Expr3, " +
                   "anschrift.ans_teilort, " +
                   "'@' AS Expr4, " +
                   "anschrift.strasse, " +
                   "'@' AS Expr5, " +
                   "'Hausnummer' AS Hausnummer," +
                   "'@' AS Bundesland, " +
                   "anschrift.ans_pf_plz, " +
                   "anschrift.ans_postfach, " +
                   "'Ja' AS Expr8, " +
                   "anschrift.ans_email, " +
                   "anschrift.ans_homepage, " +
                   "anschrift.ans_telex, " +
                   "anschrift.ans_telefon, " +
                   "'' as 'Autotelefon'," +             //28
                   "anschrift.ans_telefax, " +
                   "'' as 'Telefon2'," +
                   "'' as 'Telefax2'," +
                   "'' as 'Längengrad'," +
                   "'' as 'Breitengrad'," +
                   "anschrift.anssuch AS Expr10, " +
                   "anschrift.ans_suwo2 AS Expr11, " +
                   "'@' AS 'Verteileruppe'," +
                   "kunden.kdn_x_branche, " +
                   "'@' AS 'Region'," +               //38
                   "'@', " +
                   "kdn_sprnr, " +                   //40
                   "'' as 'ABC-Klasse'," +
                   "'@' AS 'Teilestatistik', " +
                   "'@' AS 'Webshop', " +
                   "'@' AS 'Bestandsfaktor', " +
                   "'@' AS 'Lagerort', " +
                   "kunden.kdn_erstellt, " +
                   "persktn.pkt_kdn_lief_nr ," +
                   "anschrift.ans_stnr, " +          //48
                   "'0'," +
                   "anschrift.ans_ustid, " +         //50
                   "'@' AS 'Rechnungsintervall', " +
                   "kunden.kdn_faktkz, " +
                   "persktn.pkt_rgzahler, " +                      //53
                   "'@' AS  'Konzern', " +
                   "'@' AS 'Bezeichnung', " +
                   "kunden.kdn_ekvnr, " +                          //56          
                   "(select f1e_x_ekname from f1ekverband where f1ekverband.f1e_x_eknr = kunden.kdn_ekvnr limit 1) AS f1ekname, " +
                   "'@' AS Expr24, " +                             //55
                   "'@' AS Expr25, " +
                   "'@' AS Expr26, " +
                   "'@' AS Expr27, " +
                   "'@' AS Expr28, " +
                   "'1', " +                                       //63
                   "'2', " +
                   "'3', " +
                   "'4', " +
                   "'5', " +
                   "'6', " +
                   "'3' as 'Zahlungsart', " +                      //69
                   "'@' AS Expr36, " +
                   "kdn_zbnr, " +                                  //71    
                   "kunden.kdn_kredlimit, " +                      //72
                   "kdn_c_ohneklm, " +                             //73
                   "'@' AS 'Mahnverfahren', " +                    //74
                   "'@' AS Expr39, " +                             //75
                   "'@' AS Expr40, " +                             //76
                   "'@' AS Expr41, " +                             //77
                   "kunden.kdn_vsanr, " +                          //78
                   "kdn_lbdnr," +                                        //79
                   "'@' as 'Lieferrestriktion',  " +               //80    
                   "'' as 'Vertrags'," +                                          //81
                   "'@' AS 'KredLimit WKV', " +
                   "kunden.kdn_kredvers, " +
                   "'@' AS 'WKV-Kennzeichen', " +
                   "'@' as 'l. Auskunft', " +
                   "kunden.kdn_x_adrsel, " +                       //86
                   "kdn_x_adrsel||'_'||anschrift.land||'_'||kdn_vertrnr_001, " +//87
                   "kdn_x_rabattausw, " +                         //88
                   "'Ja' AS Expr50, " +                           //89
                   "'Ja' AS Expr51, " +                           //90
                   "'@' AS Expr52, " +                            //91 
                   "'0'as 'Sammellieferschein', " +               //92 
                   "kdn_x_ust_wann," +                            //93
                   "'' as 'Bankverbindung'," +
                   "'Ja' as 'Default'," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "kdn_rekopie," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' as 'Versandkalender'," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@' ," +
                   "'@', " +
                   "'@' "+          //145
                "FROM kunden, anschrift,persktn,historie " +
                          "WHERE (persktn.pkt_ansnr = anschrift.ansnr) " +
                          "AND (anschrift.ansnr = kunden.kdn_lfdnr) " +
                          "and (kunden.kdn_sperrkz = 0) " +
                          "and his_kdnlfdnr = kdn_lfdnr " +
                          "and kdn_typ = 'D' " +
                          "and kdn_aenderung > '{0}'"

    },
            {"DU_Lieferant","Select "+
                    "kdn_kontonr," +
                    "''," +
                    "name_001," +
                    "land," +
                    "ort," +
        /*05*/      "anssuch," +
                    "ans_suwo2," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "name_002," +
                    "name_003," +
                    "plz," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "strasse," +
                    "'@'," +
                    "'@'," +
        /*20*/      "'@'," +
                    "ans_pf_plz," +
                    "ans_postfach," +
                    "'ja'," +
                    "ans_email," +
                    "ans_homepage," +
                    "ans_telex," +
                    "ans_telefon," +
                    "ans_telefax," +
                    "'@'," +
                    "anssuch," +
                    "ans_suwo2," +
                    "''," +
                    "kdn_x_branche," +
                    "'@'," +
                    "'@'," +
                    "kdn_sprnr," +
                    "'@'," +
                    "kdn_kredkontonr," +
                    "ans_stnr," +
        /*40*/      "'0'," +
                    "ans_ustid," +
                    "'@'," +
                    "'@'," +
        /*44*/      "kdn_zbnr," +
                    "kdn_kredlimit," +
                    "'1'," +
                    "'2'," +
                    "'3'," +
                    "'4'," +
                    "'5'," +
                    "'6'," +
        /*52*/      "kdn_lbdnr," +
                    "kdn_vsanr," +
                    "kdn_c_minbst," +
                    "'@'," +
                    "kdn_abkopie," +
                    "'@'," +
                    "kdn_x_ust_wann " +
                    "from anschrift, kunden " +
                    "where ansnr = kdn_lfdnr " +
                    "and ans_typ = 'K' " +
                    "and kdn_kontonr not like ('82013') " +
                    "and kdn_info_001 not like ('gelöscht') " +
                    //"and kdn_info_001 not like ('gesperrt') " +
                    "and ans_aenderung > {0} " +
                    "and ans_aenderung < {1} " +
                    "group by kdn_kontonr" },

            { "Kunden_ASCII",
                "Select " +
                "kdn_kontonr,kdn_zbnr, " +
                "ans_ustid," +
                "kdn_ekvnr, " +
                "(select f1e_x_ekname from f1ekverband where f1e_x_eknr = kdn_ekvnr limit 1) " +
                "from kunden,anschrift " +
                "where kdn_lfdnr = ansnr " +
                "and kdn_typ = 'D' " +
                "group by kdn_kontonr "},

            {"Lieferanten_ASCII","select " +
                    "kdn_kontonr," +
                    "name_001," +
                    "land," +
                    "kdn_zbnr," +
                    "ans_ustid " +
                    "from kunden,anschrift " +
                    "where kdn_lfdnr = ansnr " +
                    "and kdn_kontonr not like ('82013') " +
                    "and kdn_info_001 not like ('gelöscht') " +
                    "and kdn_typ = 'K' " +
                    "group by kdn_kontonr "},

            {"Belege_ASCII","select " +
                "bel_nr," +
                "bel_zbnr," +
                "kdn_kontonr, " +
                "his_renr " +
                "from beleg,kunden,historie " +
                "where bel_datum > 20221231 " +
                "and kdn_kontonr = bel_kontonr " +
                "and his_belnr = bel_nr " +
                "and bel_typ = '1' " +
                "group by bel_nr " }
        };

        //SQL-Queries HBS
        public Dictionary<string, string> QueriesHBS = new Dictionary<string, string>()
        {
            {"DU_Kunde","select " +
                     "kdn_kontonr," +
                    "''," +
                    "name_001," +
                    "land," +
                    "ort," +
        /*05*/      "anssuch," +
                    "ans_suwo2," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "name_002," +
                    "name_003," +
                    "plz," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "strasse," +
                    "'@'," +
                    "'@'," +
        /*20*/      "'@'," +
                    "ans_pf_plz," +
                    "ans_postfach," +
                    "'ja'," +
                    "ans_email," +
                    "ans_homepage," +
                    "ans_telex," +
                    "ans_telefon," +
                    "ans_telefax," +
                    "'@'," +
                    "anssuch," +
                    "ans_suwo2," +
                    "''," +
                    "kdn_x_branche," +
                    "'@'," +
                    "'@'," +
                    "kdn_sprnr," +
                    "'@'," +
                    "kdn_kredkontonr," +
                    "ans_stnr," +
        /*40*/      "'0'," +
                    "ans_ustid," +
                    "'@'," +
                    "'@'," +
        /*44*/      "kdn_zbnr," +
                    "kdn_kredlimit," +
                    "'1'," +
                    "'2'," +
                    "'3'," +
                    "'4'," +
                    "'5'," +
                    "'6'," +
        /*52*/      "kdn_lbdnr," +
                    "kdn_vsanr," +
                    "kdn_c_minbst," +
                    "'@'," +
                    "kdn_abkopie," +
                    "'@'," +
                    "kdn_x_ust_wann " +
                    "from anschrift,kunden " +
                    "where ansnr = kdn_lfdnr " +
                    "and ans_typ = 'K' " +
                    "and kdn_info_001 not like ('gelöscht') " +
                    "and kdn_info_001 not like ('gesperrt') " +
                    "and kdn_aenderung > '{0}'"},

            {"DU_Lieferant","SELECT " +
                       "kunden.kdn_kontonr," +                         //0
                       "'@' as Profitcenter, " +
                       "'', " +
                       "anschrift.name_001, " +
                       "anschrift.land, " +
                       "anschrift.ort, " +
                       "anschrift.anssuch, " +
                       "anschrift.ans_suwo2, " +
                       "'@', " +
                       "'@'," +
                       "'@', " +                              //10
                       "'@', " +
                       "anschrift.name_002," +
                       "anschrift.name_003, " +
                       "anschrift.plz," +
                       " '@' AS Expr3, " +
                       "anschrift.ans_teilort, " +
                       "'@' AS Expr4, " +
                       "anschrift.strasse, " +
                       "'@' AS Expr5, " +
                       "'Hausnummer' AS Hausnummer," +                 //20
                       "'@' AS Bundesland, " +
                       "anschrift.ans_pf_plz, " +
                       "anschrift.ans_postfach, " +
                       "'Ja' AS Expr8, " +
                       "anschrift.ans_email, " +
                       "anschrift.ans_homepage, " +
                       "anschrift.ans_telex, " +
                       "anschrift.ans_telefon, " +
                       "anschrift.ans_telefax, " +
                       "'@' AS Expr9, " +                              //30
                       "anschrift.anssuch AS Expr10, " +
                       "anschrift.ans_suwo2 AS Expr11, " +
                       "'@' AS Expr12," +
                       "kunden.kdn_x_branche, " +
                       "'@' AS Expr13, " +
                       "'@', " +                                       //Sachbearbeiter auf @ gesetzt, da nicht alle Sachbearbeiter vorhanden
                       "kdn_sprnr, " +
                       "'@' AS Expr14, " +
                       "'@' AS Expr15, " +
                       "'@' AS Expr16, " +                             //40
                       "'@' AS Expr17, " +
                       "'@' AS Expr18, " +
                       "kunden.kdn_erstellt, " +
                       "'', " +
                       "anschrift.ans_stnr, " +
                       "'0'," +
                       "anschrift.ans_ustid, " +
                       "'@' AS Expr19, " +
                       "kunden.kdn_faktkz, " +
                       "persktn.pkt_rgzahler, " +                      //50 
                       "'@' AS Expr21, " +
                       "'@' AS Expr22, " +
                       "kunden.kdn_ekvnr, " +
                       "(select f1e_x_ekname from f1ekverband where f1ekverband.f1e_x_eknr = kunden.kdn_ekvnr limit 1) AS f1ekname, " +
                       "'@' AS Expr24, " +                             //55
                       "'@' AS Expr25, " +
                       "'@' AS Expr26, " +
                       "'@' AS Expr27, " +
                       "'@' AS Expr28, " +
                       "'1', " +                                       //60
                       "'2', " +
                       "'3', " +
                       "'4', " +
                       "'5', " +
                       "'6', " +
                       "'3', " +
                       "'@' AS Expr36, " +
                       "kdn_zbnr, " +
                       "kunden.kdn_kredlimit, " +                      //70
                       "kdn_c_ohneklm, " +                             //71 Kreditlimitprüfung(19.10.2023 M.Peek : Wenn Haken gesetzt = JA , andernfalls = NEIN) 
                       "'@' AS Expr38, " +
                       "'@' AS Expr39, " +
                       "'@' AS Expr40, " +
                       "'@' AS Expr41, " +
                       "'@' AS Expr42, " +
                       "kunden.kdn_vsanr, " +                          //76
                       "'1', " +                                       //77
                       "'1', " +                                       //78 Lieferrestriktion (19.10.23 M.Peek : 1 - Keine Teillieferung)
                       "'@', " +
                       "'@' AS Expr46, " +                             //80
                       "kunden.kdn_kredvers, " +
                       "'@' AS Expr48, " +
                       "'@' AS Expr49, " +
                       "kunden.kdn_x_adrsel, " +                       //84
                       "kdn_x_adrsel||'_'||anschrift.land||'_'||kdn_vertrnr_001, " +
                       "kdn_x_rabattausw, " +
                       "'Ja' AS Expr50, " +
                       "'Ja' AS Expr51, " +
                       "'@' AS Expr52, " +
                       "'', " +                        //90
                       "'', " +
                       "'0', " +
                       "kdn_x_ust_wann " +
                       "FROM kunden, anschrift, persktn " +
                       "WHERE (persktn.pkt_ansnr = anschrift.ansnr) " +
                       "AND (anschrift.ansnr = kunden.kdn_lfdnr) " +
                       "and (kunden.kdn_sperrkz = 0) " +
                       "and ans_typ = 'D' " +
                       "and ans_aenderung > '{0}'"},

            {"Lieferanten_ASCII","select " +
                    "kdn_kontonr," +
                    "name_001," +
                    "land," +
                    "kdn_zbnr," +
                    "ans_ustid " +
                    "from kunden,anschrift " +
                    "where kdn_lfdnr = ansnr " +
                    "and kdn_typ = 'K' " +
                    "and kdn_kontonr not like '99999' " +
                    "group by kdn_kontonr " +
                ""},

            {"Kunden_ASCII","select " +
                   "kdn_kontonr," +
                   "kdn_zbnr," +
                   "ans_ustid," +
                   "kdn_ekvnr, " +
                   "(select f1e_x_ekname from f1ekverband where f1e_x_eknr = kdn_ekvnr limit 1) " +
                   "from kunden,anschrift " +
                   "where kdn_lfdnr = ansnr " +
                   "and kdn_typ = 'D' " +
                   "group by kdn_kontonr " +
                   ""
            }
        };

    
        // Kunden für transASCIIact
        public DataTable GetKundenASCII(OdbcConnection connection, string standort)
        {
            ConnectionWatch(connection);
            OdbcConnection conn = new OdbcConnection(connection.ConnectionString);
            DataTable dataTable = new DataTable();
            using (OdbcCommand command = new OdbcCommand(GetDataKunden(standort), conn))
            {
                dataTable = WriteTable(command.CommandText, dataTable, conn);
            }

            dataTable.TableName = "Kunden_ASCII";
            return dataTable;
        }

        // Lieferanten für transASCIIact
        public DataTable GetASCIITable(OdbcConnection connection, string query)
        {
            ConnectionWatch(connection);
            DataTable dataTable = new DataTable();
            OdbcConnection conn = new OdbcConnection(connection.ConnectionString);
            using (conn)
            {
                conn.Open();
                using (OdbcCommand command = new OdbcCommand(query, conn))
                {
                    dataTable.Load(command.ExecuteReader());
                }

                dataTable.TableName = "Lieferanten_ASCII";
                return dataTable;
            }
        }

        // DataTable der Belege für transASCIIact
        public DataTable GetBelegeASCII(OdbcConnection connection)
        {
            ConnectionWatch(connection);
            DataTable dataTable = new DataTable();
            OdbcConnection conn = new OdbcConnection(connection.ConnectionString);

            using (OdbcCommand command = new OdbcCommand(BelegeString(), conn))
            {
                dataTable = WriteTable(command.CommandText, dataTable, conn);
            }

            dataTable.TableName = "Belege_ASCII";

            return dataTable;
        }

        //Methode zur Ausgabe einer .csv Datei, erstellt aus einer DataTable

        public void Table2CSV(DataTable table, string path, string preString)
        {
            string filePathtrue = path + preString + table.TableName + ".csv";
            string cols = string.Empty;
            FileInfo pathFileInfo = new FileInfo(filePathtrue);

            if (pathFileInfo.Exists)
            {
                string newInfo = pathFileInfo.FullName;
                newInfo.Replace("I.csv", "I001.csv");
            }

            using (FileStream fsTrue = File.OpenWrite(pathFileInfo.FullName))
            using (StreamWriter streamWriterTrue = new StreamWriter(fsTrue, Encoding.UTF8))
            {
                if (table.Columns.Count > 0)
                {
                    foreach (DataColumn column in table.Columns)
                    {
                        cols += column.ColumnName.ToString() + ";";
                    }

                    cols = cols.Substring(0, cols.Length - 1);
                    streamWriterTrue.WriteLine(cols);

                    foreach (DataRow row in table.Rows)
                    {
                        string line = "";

                        foreach (var entry in row.ItemArray)
                        {
                            line += entry.ToString() + ";";
                        }

                        line = line.Substring(0, line.Length - 1);
                        streamWriterTrue.WriteLine(line);
                    }
                }
            }
        }


        //Methode zum überwachen der ODBC Verbindung 
        public static void ConnectionWatch(OdbcConnection con)
        {
            if (con.ConnectionString == string.Empty)
            {
                con = OdbcSDL;
            }

            ConnectionState state = con.State;
            try
            {
                con.Close();
            }
            catch (Exception e)
            {

            }


            if (state == ConnectionState.Open)
            {
                Task.Delay(500);
            }
            else if (con.State == ConnectionState.Closed)
            {
                con.Close();
                //.Text += "Verbindung geschlossen";
                //errorText.BringIntoView();
                string text = con.ConnectionString;
                con.Open();

            }

        }

        // Methode zum Schreiben der Tabelle
        public static DataTable WriteTable(string query, DataTable table, OdbcConnection conn)
        {
            table = new DataTable(table.TableName);

            using (OdbcCommand command = new OdbcCommand(query, conn))
            using (OdbcDataAdapter adapter = new OdbcDataAdapter(command))
            {
                command.CommandTimeout = 30;
                try
                {
                    Stopwatch stopwatch = new Stopwatch();
                    stopwatch.Start();
                    adapter.SelectCommand = command;
                    table.Load(command.ExecuteReader());
                    stopwatch.Stop();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    Console.ReadLine();
                }

                DataTable clone = table.Clone();
                int i = 0;

                foreach (DataColumn col in clone.Columns)
                {
                    col.ReadOnly = false;
                    col.DataType = typeof(string);
                }

                foreach (DataRow row in table.Rows)
                {
                    clone.ImportRow(row);
                }

                clone.TableName = table.TableName;

                return clone;
            }
        }




        // SQL-Abfrage Kunden
        public static string KundenString(string datum, string standort)
        {
            string query = string.Empty;

            if (standort == "1")
            {
                query = "SELECT " +
                          "kunden.kdn_kontonr," +                         //0
                          "'@' as Profitcenter, " +
                          "'', " +
                          "anschrift.name_001, " +
                          "anschrift.land, " +
                          "anschrift.ort, " +
                          "anschrift.anssuch, " +
                          "anschrift.ans_suwo2, " +
                          "'@', " +
                          "'@'," +
                          "'@', " +                              //10
                          "'@', " +
                          "anschrift.name_002," +
                          "anschrift.name_003, " +
                          "anschrift.plz," +
                          " '@' AS Expr3, " +
                          "anschrift.ans_teilort, " +
                          "'@' AS Expr4, " +
                          "anschrift.strasse, " +
                          "'@' AS Expr5, " +
                          "'Hausnummer' AS Hausnummer," +                 //20
                          "'@' AS Bundesland, " +
                          "anschrift.ans_pf_plz, " +
                          "anschrift.ans_postfach, " +
                          "'Ja' AS Expr8, " +
                          "anschrift.ans_email, " +
                          "anschrift.ans_homepage, " +
                          "anschrift.ans_telex, " +
                          "anschrift.ans_telefon, " +
                          "anschrift.ans_telefax, " +
                          "'@' AS Expr9, " +                              //30
                          "anschrift.anssuch AS Expr10, " +
                          "anschrift.ans_suwo2 AS Expr11, " +
                          "'@' AS Expr12," +
                          "kunden.kdn_x_branche, " +
                          "'@' AS Expr13, " +
                          "'@', " +                                       //Sachbearbeiter auf @ gesetzt, da nicht alle Sachbearbeiter vorhanden
                          "kdn_sprnr, " +
                          "'@' AS Expr14, " +
                          "'@' AS Expr15, " +
                          "'@' AS Expr16, " +                             //40
                          "'@' AS Expr17, " +
                          "'@' AS Expr18, " +
                          "kunden.kdn_erstellt, " +
                          "'', " +
                          "anschrift.ans_stnr, " +
                          "'0'," +
                          "anschrift.ans_ustid, " +
                           "'@' AS Expr19, " +
                           "kunden.kdn_faktkz, " +
                           "persktn.pkt_rgzahler, " +                      //50 
                          "'@' AS Expr21, " +
                          "'@' AS Expr22, " +
                          "kunden.kdn_ekvnr, " +
                          "(select f1e_x_ekname from f1ekverband where f1ekverband.f1e_x_eknr = kunden.kdn_ekvnr limit 1) AS f1ekname, " +
                          "'@' AS Expr24, " +                             //55
                          "'@' AS Expr25, " +
                          "'@' AS Expr26, " +
                          "'@' AS Expr27, " +
                          "'@' AS Expr28, " +
                          "'1', " +                                       //60
                          "'2', " +
                          "'3', " +
                          "'4', " +
                          "'5', " +
                          "'6', " +
                          "'3', " +
                          "'@' AS Expr36, " +
                          "kdn_zbnr, " +
                          "kunden.kdn_kredlimit, " +                      //70
                          "'Ja', " +
                          "'@' AS Expr38, " +
                          "'@' AS Expr39, " +
                          "'@' AS Expr40, " +
                          "'@' AS Expr41, " +
                          "'@' AS Expr42, " +
                          "kunden.kdn_vsanr, " +                          //76
                          "'1', " +                          //77
                          "'2', " +
                          "'@', " +
                          "'@' AS Expr46, " +                             //80
                          "kunden.kdn_kredvers, " +
                          "'@' AS Expr48, " +
                          "'@' AS Expr49, " +
                          "kunden.kdn_x_adrsel, " +                       //84
                          "kdn_x_adrsel||'_'||anschrift.land||'_'||kdn_vertrnr_001, " +
                          "kdn_x_rabattausw, " +
                          "'@' AS Expr50, " +
                          "'@' AS Expr51, " +
                          "'@' AS Expr52, " +
                          "kunden.kdn_rekopie, " +                        //90
                          "kunden.kdn_lskopie, " +
                          "'0', " +
                          "kdn_x_ust_wann " +
                          "FROM kunden, anschrift,persktn " +
                          "WHERE (persktn.pkt_ansnr = anschrift.ansnr) " +
                          "AND (anschrift.ansnr = kunden.kdn_lfdnr) " +
                          "and (kunden.kdn_sperrkz = 0) " +
                          "and kdn_typ = 'D' " +
                          "and kdn_aenderung > {0} " +
                          "limit 10 " +
                          "GROUP BY kdn_kontonr " +
                          "";
            }

            return query;
        }
        // SQL-Abfrage Lieferanten
        public static string LieferantenString(string datum, string standort)
        {
            string query = string.Empty;

            if (standort == "1")
            {
                query =
                 "select " +
                 "kdn_kontonr," +
                 "''," +
                 "name_001," +
                 "land," +
                 "ort," +
     /*05*/      "anssuch," +
                 "ans_suwo2," +
                 "'@'," +
                 "'@'," +
                 "'@'," +
                 "'@'," +
                 "name_002," +
                 "name_003," +
                 "plz," +
                 "'@'," +
                 "'@'," +
                 "'@'," +
                 "strasse," +
                 "'@'," +
                 "'@'," +
     /*20*/      "'@'," +
                 "ans_pf_plz," +
                 "ans_postfach," +
                 "'ja'," +
                 "ans_email," +
                 "ans_homepage," +
                 "ans_telex," +
                 "ans_telefon," +
                 "ans_telefax," +
                 "'@'," +
                 "anssuch," +
                 "ans_suwo2," +
                 "'@'," +
                 "kdn_x_branche," +
                 "'@'," +
                 "'@'," +
                 "kdn_sprnr," +
                 "'@'," +
                 "kdn_kredkontonr," +
                 "ans_stnr," +
     /*40*/      "'0'," +
                 "ans_ustid," +
                 "'@'," +
                 "'@'," +
     /*44*/      "kdn_zbnr," +
                 "kdn_kredlimit," +
                 "'1'," +
                 "'2'," +
                 "'3'," +
                 "'4'," +
                 "'5'," +
                 "'6'," +
     /*52*/      "kdn_lbdnr," +
                 "kdn_vsanr," +
                 "kdn_c_minbst," +
                 "'@'," +
                 "kdn_abkopie," +
                 "'@'," +
                 "kdn_x_ust_wann " +
                 "from anschrift, kunden " +
                 "where ansnr = kdn_lfdnr " +
                 "and kdn_aenderung > " + datum + " " +
                 " ";
            }

            if (standort == "2")
            {
                query =
                    "select " +
                    "kdn_kontonr," +
                    "''," +
                    "name_001," +
                    "land," +
                    "ort," +
        /*05*/      "anssuch," +
                    "ans_suwo2," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "name_002," +
                    "name_003," +
                    "plz," +
                    "'@'," +
                    "'@'," +
                    "'@'," +
                    "strasse," +
                    "'@'," +
                    "'@'," +
        /*20*/      "'@'," +
                    "ans_pf_plz," +
                    "ans_postfach," +
                    "'ja'," +
                    "ans_email," +
                    "ans_homepage," +
                    "ans_telex," +
                    "ans_telefon," +
                    "ans_telefax," +
                    "'@'," +
                    "anssuch," +
                    "ans_suwo2," +
                    "'@'," +
                    "kdn_x_branche," +
                    "'@'," +
                    "'@'," +
                    "kdn_sprnr," +
                    "'@'," +
                    "kdn_kredkontonr," +
                    "ans_stnr," +
        /*40*/      "'0'," +
                    "ans_ustid," +
                    "'@'," +
                    "'@'," +
        /*44*/      "kdn_zbnr," +
                    "kdn_kredlimit," +
                    "'1'," +
                    "'2'," +
                    "'3'," +
                    "'4'," +
                    "'5'," +
                    "'6'," +
        /*52*/      "kdn_lbdnr," +
                    "kdn_vsanr," +
                    "kdn_c_minbst," +
                    "'@'," +
                    "kdn_abkopie," +
                    "'@'," +
                    "kdn_x_ust_wann " +
                    "from anschrift,kunden " +
                    "where ansnr = kdn_lfdnr " +
                    "and kdn_typ = 'K' " +
                    "and kdn_info_001 not like ('gelöscht') " +
                    "and kdn_info_001 not like ('gesperrt') " +
                    " ";
            }

            return query;
        }
        
        public string[] queries = new string[]
        {
                    "select " +
                    "kdn_kontonr," +
                    "kdn_zbnr," +
                    "ans_ustid," +
                    "kdn_ekvnr, " +
                    "(select f1e_x_ekname from f1ekverband where f1e_x_eknr = kdn_ekvnr limit 1)  " +
                    "from kunden,anschrift,historie " +
                    "where kdn_lfdnr = ansnr " +
                    "and kdn_typ = 'D' "+
                    "limit 10 " ,
                    "select " +
                    "kdn_kontonr," +
                    "name_001," +
                    "land," +
                    "kdn_zbnr," +
                    "ans_ustid " +
                    "from kunden,anschrift " +
                    "where kdn_lfdnr = ansnr " +
                    "and kdn_kontonr not like ('82013') " +
                    "and kdn_info_001 not like ('gelöscht') " +
                    "and kdn_typ = 'K' " +
                    "limit 10 ",
                    "select bel_nr,bel_zbnr,kdn_kontonr, his_renr " +
                    "from beleg,kunden,historie " +
                    "where bel_datum > 20221231 and kdn_kontonr = bel_kontonr and his_belnr = bel_nr and bel_typ = '1' " +
                    ""
        };

        //SQL-Abfrage Kunden transASCIIact
        public static string GetDataKunden(string standort)
        {
            string query = string.Empty;

            if (standort == "1")
            {
                query = "select " +
                    "kdn_kontonr," +
                    "kdn_zbnr," +
                    "ans_ustid," +
                    "kdn_ekvnr, " +
                    "(select f1e_x_ekname from f1ekverband where f1e_x_eknr = kdn_ekvnr limit 1)  " +
                    "from kunden,anschrift,historie " +
                    "where kdn_lfdnr = ansnr " +
                    "and kdn_typ = 'D' " +
                    " ";
            }
            else if (standort == "2")
            {
                query = "select " +
                   "kdn_kontonr," +
                   "kdn_zbnr," +
                   "ans_ustid," +
                   "kdn_ekvnr, " +
                   "(select f1e_x_ekname from f1ekverband where f1e_x_eknr = kdn_ekvnr limit 1) " +
                   "from kunden,anschrift " +
                   "where kdn_lfdnr = ansnr " +
                   "and kdn_typ = 'D' " +
                   " ";

            }

            return query;
        }

        // SQL-Abfrage Lieferanten für transASCIIact
        public static string GetDataLieferanten(string standort)
        {
            string query = string.Empty;

            if (standort == "1")
            {
                query = "select " +
                    "kdn_kontonr," +
                    "name_001," +
                    "land," +
                    "kdn_zbnr," +
                    "ans_ustid " +
                    "from kunden,anschrift " +
                    "where kdn_lfdnr = ansnr " +
                    "and kdn_kontonr not like ('82013') " +
                    "and kdn_info_001 not like ('gelöscht') " +
                    "and kdn_typ = 'K' " +
                    " ";
            }

            if (standort == "2")
            {
                query = "select " +
                    "kdn_kontonr," +
                    "name_001," +
                    "land," +
                    "kdn_zbnr," +
                    "ans_ustid " +
                    "from kunden,anschrift " +
                    "where kdn_lfdnr = ansnr " +
                    "and kdn_typ = 'K'" +
                    " ";
            }
            return query;
        }


        // SQL-Abfrage Belege mit Zahlungsbedingung
        public static string BelegeString()
        {
            string query = "select " +
                "bel_nr," +
                "bel_zbnr," +
                "kdn_kontonr, " +
                "his_renr " +
                "from beleg,kunden,historie " +
                "where bel_datum > 20221231 " +
                "and kdn_kontonr = bel_kontonr " +
                "and his_belnr = bel_nr " +
                "and bel_typ = '1' " +
                "";

            return query;
        }

    }
}





