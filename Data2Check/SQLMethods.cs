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

namespace Data2Check
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

        public string GetStandort(OdbcConnection connection)
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

        //SQL-Queries SDL
        public Dictionary<string, string> QueriesSDL = new Dictionary<string, string>()
        {

        {"DU_Kunde",$"SELECT " +
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
                    "'', " +          //48
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
                    "'2' AS 'Mahnverfahren', " +                    //74
                    "'@' AS Expr39, " +                             //75
                    "'@' AS Expr40, " +                             //76
                    "'@' AS Expr41, " +                             //77
                    "kunden.kdn_vsanr, " +                          //78
                    "kdn_lbdnr," +                                        //79
                    "'@' as 'Lieferrestriktion',  " +               //80    
                     "'' as 'Vertrags'," +                                          //81
                    "'@' AS 'KredLimit WKV', " +
                    "kunden.kdn_kredvers, " +                       //83
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
                    "'1' ," +
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
                    "'@' " +           //145
                    "FROM kunden, anschrift,persktn " +
                    "WHERE (persktn.pkt_ansnr = anschrift.ansnr) " +
                    "AND (anschrift.ansnr = kunden.kdn_lfdnr) " +
                    "and kdn_typ = 'D' " +
                    //U"and kdn_kontonr = 10093"
                    "and kdn_le_redat > 20240411 " +
                    "and kdn_le_redat < 20240416 "+
            //"and kdn_kontonr in (30025,43681,33759,26881,60270,11527,12805,45467,59547,49821,36093,18317,40189,38669,17415,44489,27747,52496,36792,43643,25066)";
            //"and kdn_kontonr in (15921,	49848,	11106,	28443,	60234,	32742,	49994,	45592,	42981,	28564,	50495,	27511,	12174,	35948,	23724,	20664,	28495,	56065,	49216,	34617,	31328,	36686,	49399,	43617,	27456,	17007,	43362,	46608,	57873,	21118) ";
                   // "and kdn_kontonr  in (11692, 31364) ";
            //"and kdn_aenderung > 20240406 " +
            //"and kdn_aenderung < 20240411 ";
                "and kdn_le_redat > {DatStart} "+
                "and kdn_le_redat > {DatEnd} "+
                "GROUP BY kdn_kontonr "

        },
        {"DU_Lieferant",$"SELECT " +
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
                    "'', " +          //48
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
                    "'2' AS 'Mahnverfahren', " +                    //74
                    "'@' AS Expr39, " +                             //75
                    "'@' AS Expr40, " +                             //76
                    "'@' AS Expr41, " +                             //77
                    "kunden.kdn_vsanr, " +                          //78
                    "kdn_lbdnr," +                                        //79
                    "'@' as 'Lieferrestriktion',  " +               //80    
                     "'' as 'Vertrags'," +                                          //81
                    "'@' AS 'KredLimit WKV', " +
                    "kunden.kdn_kredvers, " +                       //83
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
                    "'1' ," +
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
                    "'@' " +           //145
                    "FROM kunden, anschrift,persktn " +
                    "WHERE (persktn.pkt_ansnr = anschrift.ansnr) " +
                    "AND (anschrift.ansnr = kunden.kdn_lfdnr) " +
                    "and kdn_typ = 'D' " +
                    //U"and kdn_kontonr = 10093"
                    "and kdn_le_redat > {DatStart} " +
                    "and kdn_le_redat < {DatEnd} "+
                    "group by kdn_kontonr"
            //"and kdn_kontonr in (30025,43681,33759,26881,60270,11527,12805,45467,59547,49821,36093,18317,40189,38669,17415,44489,27747,52496,36792,43643,25066)";
            //"and kdn_kontonr in (15921,	49848,	11106,	28443,	60234,	32742,	49994,	45592,	42981,	28564,	50495,	27511,	12174,	35948,	23724,	20664,	28495,	56065,	49216,	34617,	31328,	36686,	49399,	43617,	27456,	17007,	43362,	46608,	57873,	21118) ";
                    
            //"and kdn_aenderung > 20240406 " +
            //"and kdn_aenderung < 20240411 ";
                        
        },

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
        
        {"Belege_ASCII","SELECT " +
            "bel_nr," +
            "bel_zbnr," +
            "kdn_kontonr," +
            "his_renr, " +
            "pkt_rgzahler " +
            "FROM beleg,kunden,historie,persktn " +
            "WHERE bel_datum > 20221231 " +
            "AND kdn_kontonr NOT IN(98888,77203) " +
            "AND pkt_ktonr = kdn_kontonr " +
            "AND pos_bellfdnr = bel_lfdnr " +
            "AND kdn_kontonr = bel_kontonr " +
            "AND pos_artlfdnr = art_lfdnr " +
            "group BY his_renr"
        }
    };

        //SQL-Queries HBS
        public Dictionary<string, string> QueriesHBS = new Dictionary<string, string>()
        {
          
    

            {"DU_Lieferant","select " +
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
                        "'9999'," +
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
                        "and kdn_le_redat > 20240411 " +
                        "and kdn_le_redat < 20240416 "
                        //"and kdn_kontonr in (77322,77934)";
                       },

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
                    "group by kdn_kontonr " 
            },

        };
    
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

        // Belege(Einkauf) zum Abgleich der Zahlungsbedingung mit Rechnungsnummer


        // SQL-Abfrage Kunden
        public static string KundenString(string datum, string standort)
        {
            string query = string.Empty;

            if (standort == "1")
            {
                query = @"SELECT " +
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
                   "'@' " +          //145
                "FROM kunden, anschrift,persktn " +
                          "WHERE (persktn.pkt_ansnr = anschrift.ansnr) " +
                          "AND (anschrift.ansnr = kunden.kdn_lfdnr) " +
                          "and (kunden.kdn_sperrkz = 0) " +
                          "and kdn_typ = 'D' " +
                          "and kdn_le_redat > {datum} " +
                          "GROUP BY kdn_kontonr ";
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
                        "anssuch," +
                        "ans_suwo2," +
                        "'' as'Verteilergruppe'," +
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
                        "'9999'," +
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
                        "'@'," +
                        "'@'," +
                        "kdn_x_ust_wann," +
                        "''as 'Bankverbindung'," +
                        "'ja'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@', " +
                        "'@'," +
                        "'@' " +
                 "from anschrift, kunden " +
                 "where ansnr = kdn_lfdnr " +
                 "and kdn_aenderung > " + datum + " " +
                 " ";
            }

            if (standort == "2")
            {
                query =
                   "select " +
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
                        "anssuch," +
                        "ans_suwo2," +
                        "'' as'Verteilergruppe'," +
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
                        "'9999'," +
            /*44*/      "kdn_zbnr," +
                        "kdn_kredlimit," +
                        "'1'," +                            //50
                        "'2'," +
                        "'3'," +
                        "'4'," +
                        "'5'," +
                        "'6'," +
            /*52*/      "kdn_lbdnr," +
                        "kdn_vsanr," +
                        "kdn_c_minbst," +
                        "'@'," +
                        "'@'," +
                        "'0'," +
                        "kdn_x_ust_wann," +
                        "''as 'Bankverbindung'," +
                        "'ja'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@'," +
                        "'@', " +
                        "'@', " +
                        "'@' " +
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
                    "and kdn_typ = 'D' "
            ,
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
                    "and kdn_typ = 'K' " 
            ,
                    "select bel_nr,bel_zbnr,kdn_kontonr, his_renr,pkt_rgzahler " +
                    "from beleg,kunden,historie,persktn " +
                    "where bel_datum > 20221231 and kdn_kontonr = bel_kontonr and his_belnr = bel_nr"
                    
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
                    "from kunden,anschrift, " +
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
                "his_renr," +
                "pkt_rgzahler " +
                "from beleg,kunden,historie,persktn" +
                "where bel_datum > 20221231 " +
                "and kdn_kontonr = bel_kontonr " +
                "and pkt_ktonr = kdn_kontonr " +
                "and his_belnr = bel_nr " +
                "" +
                "";

            return query;
        }
   
    }
}





