using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Data2Check
{
    class SQLMethods
    {
        static Operations Operations = new Operations();
        static Dictionaries Dictionaries = new Dictionaries();
        static DataTables DataTables = new DataTables();
        static DataTable Atradius = new DataTable();

        public SQLMethods()
        {
            Operations operations = new Operations();
            operations.FillAtradius(Atradius);
        }

        // DataTable der Lieferanten seit 'datum'
        public DataTable GetLieferanten(string datum, OdbcConnection connection, string standort)
        {
            DataTable resultTable = new DataTable("DU_Lieferant");

            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }

            OdbcCommand command = new OdbcCommand(KundenString(datum, standort), connection);
            resultTable = WriteTable(command, resultTable, DataTables.LieferantenTable);

            return resultTable;
        }

        // DataTable der Kunden seit 'datum'
        public DataTable GetKunden(string datum, OdbcConnection connection, string standort)
        {
            DataTable cloneTable = DataTables.KundenTable.Clone();
            foreach (DataColumn col in cloneTable.Columns) { col.ReadOnly = false; col.DataType = typeof(string); }
            DataTable resultTable = new DataTable("DU_Kunde");

            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }

            OdbcCommand command = new OdbcCommand(KundenString(datum, standort), connection);
            resultTable = WriteTable(command, resultTable, DataTables.KundenTable);

            foreach (DataRow rowKd in resultTable.Rows)
            {
                string ansnr = rowKd[2].ToString();

                try
                {
                    if (Atradius.Rows.Contains(rowKd[0].ToString()))
                    {
                        foreach (DataRow row in Atradius.Rows)

                        {
                            if (row[0].ToString() == rowKd[0].ToString())
                            {
                                rowKd[79] = "216426";
                                rowKd[80] = row[1];
                                rowKd[81] = row[4];
                                rowKd[82] = "B";
                                rowKd[83] = row[5];
                            }
                        }
                    }

                    else
                    {
                        rowKd[79] = "@";
                        rowKd[80] = "@";
                        rowKd[81] = "@";
                        rowKd[82] = "@";
                        rowKd[83] = "@";
                    }

                    rowKd[0] = standort + rowKd[0].ToString();
                    rowKd[7] = rowKd[5].ToString().ToUpper();
                    rowKd[34] = Operations.SetBrancheNr(rowKd[34].ToString());

                    if (rowKd[18] != null)
                    {
                        if (rowKd[4].ToString() != "FR")
                        {
                            try
                            {
                                Operations.GetHnr(rowKd[18].ToString());

                                rowKd[18] = Operations.Street;
                                rowKd[20] = Operations.HNr;
                            }
                            catch (Exception e)
                            {

                            }
                        }
                        else
                        {
                            rowKd[20] = "@";
                        }
                    }

                    rowKd[18] = Operations.CorrectStrasse(rowKd[18].ToString());
                    rowKd[39] = "Ja";

                    if (int.Parse(rowKd[43].ToString()) > 0)
                    {
                        rowKd[43] = Operations.DateConvert(rowKd[43].ToString());
                    }
                    else
                    {
                        rowKd[43] = "@";
                    }
                }

                catch (Exception ex)
                {


                    rowKd[43] = "@";
                }


                if (rowKd[50] != null && rowKd[50].ToString() != "@")
                {
                    if (rowKd[50].ToString() != "0")
                    {
                        rowKd[50] = standort + rowKd[50].ToString();
                    }

                    else
                    {
                        rowKd[50] = "@";
                    }
                }

                if (rowKd[53].ToString() != "0")
                {

                    if (Dictionaries.VerbandDict.ContainsKey(rowKd[53].ToString()))
                    {
                        rowKd[53] = Dictionaries.VerbandDict[rowKd[53].ToString()];
                    }
                    else
                    {
                        rowKd[53] = "@";
                    }
                }
                else if (rowKd[53].ToString() == "0")
                {
                    rowKd[53] = "@";
                }


                if (rowKd[53].ToString() == "195160" | rowKd[53].ToString() == "381450" | rowKd[53].ToString() == "194330" | rowKd[53].ToString() == "196930")
                {
                    rowKd[57] = Operations.GetNrVerb(rowKd[12].ToString(), rowKd[13].ToString(), rowKd[53].ToString());
                }

                if (rowKd[4].ToString().ToUpper() == "DE")
                {
                    try
                    {
                        rowKd[21] = Dictionaries.BundeslandDict[rowKd[14].ToString()];
                        rowKd[18] = Operations.CorrectStrasse(rowKd[18].ToString());
                    }

                    catch (KeyNotFoundException knfe)
                    {
                        rowKd[21] = "@";
                    }
                }

                else
                {
                    rowKd[21] = "@";
                }

                rowKd[35] = Operations.GetRegion(rowKd[4].ToString(), rowKd[14].ToString());

                if (Dictionaries.sprachenDict.ContainsKey(rowKd[37].ToString()))
                {
                    rowKd[37] = Dictionaries.sprachenDict[rowKd[37].ToString()];
                }

                else
                {
                    rowKd[37] = "@";
                }

                if (rowKd[37].ToString() == "D" | rowKd[4].ToString() == "DE" | rowKd[4].ToString() == "CH" | rowKd[4].ToString() == "AT")
                {
                    rowKd[8] = "Sehr geehrte Damen und Herren";
                }
                else if (rowKd[37].ToString() == "F" | rowKd[4].ToString() == "FR")
                {
                    rowKd[8] = "Mesdames, Messieurs";
                }
                else if (rowKd[37].ToString() == "E")
                {
                    rowKd[8] = "Dear Sir / Madam";
                }
                else if (rowKd[37].ToString() == "DU")
                {
                    rowKd[8] = "Geachte dames en heren";
                }

                else
                {
                    rowKd[8] = rowKd[8];
                }

                rowKd[47] = Operations.RemoveWhiteSpace(rowKd[47].ToString());

                if (rowKd[47].ToString() != string.Empty && rowKd[47].ToString().Length > 3)
                {
                    if (!Dictionaries.LandUstidList.Contains(rowKd[47].ToString().Substring(0, 2)))
                    {
                        rowKd[45] = rowKd[47].ToString();
                    }
                }


                if (rowKd[32].ToString() == "" | rowKd[32].ToString() == null)
                {
                    rowKd[32] = rowKd[5].ToString().ToUpper(); ;
                }

                if (standort == "2" && Dictionaries.ZbdNrHBS2SDL.ContainsKey(rowKd[68].ToString()))
                {

                    rowKd[68] = Dictionaries.ZbdNrHBS2SDL[rowKd[68].ToString()];
                }
                else if (Dictionaries.zahlungszielDict.ContainsKey(rowKd[68].ToString()))
                {
                    rowKd[68] = Dictionaries.zahlungszielDict[rowKd[68].ToString()];
                }
                else
                {
                    rowKd[68] = "@";
                }

                if (rowKd[93].ToString() != "0")
                {
                    rowKd[93] = "Ja";
                }

                else
                {
                    rowKd[93] = "Nein";
                }

                if (rowKd[49].ToString() == "0")
                {
                    rowKd[49] = "Nein";
                }

                else
                {
                    rowKd[49] = "Ja";
                }
                if (rowKd[70].ToString() == "N")
                {
                    rowKd[70] = "Nein";
                }

                else if (rowKd[70].ToString() == "J")
                {
                    rowKd[70] = "Ja";
                }
                if (rowKd[76] != null && Dictionaries.VADict.ContainsKey(rowKd[76].ToString()))
                {
                    rowKd[76] = Dictionaries.VADict[rowKd[76].ToString()];
                }

                else
                {
                    rowKd[76] = "@";
                }

                if (rowKd[84] != null)
                {
                    try
                    {
                        rowKd[84] = Operations.GetPreisliste(rowKd[84].ToString());
                    }

                    catch (KeyNotFoundException knfe)
                    {
                        rowKd[84] = "@";
                    }

                }

                if (rowKd[85] != null)
                {
                    try
                    {
                        rowKd[85] = Operations.GetKundenrabattgruppe(rowKd[85].ToString());
                    }

                    catch (KeyNotFoundException knfe)
                    {
                        rowKd[85] = "@";
                    }
                }

                rowKd[86] = "Ja";
                cloneTable.ImportRow(rowKd);
            }

            return cloneTable;
        }

        //Methode zur Ausgabe einer .csv Datei, erstellt aus einer DataTable
        public void Table2CSV(DataTable table, string path, string preString)
        {
            int i = 0;
            string filePathtrue = path + preString + table.TableName + DateTime.Now.Year.ToString() + DateTime.Now.Month.ToString() + DateTime.Now.Day.ToString() + ".csv";
            string cols = string.Empty;
            FileInfo pathFileInfo = new FileInfo(filePathtrue);

            while (pathFileInfo.Exists)
            {
                pathFileInfo = new FileInfo(path + preString + table.TableName + DateTime.Now.Date.ToString() + i + ".csv");
                i++;
            }

            using (FileStream fsTrue = File.OpenWrite(pathFileInfo.FullName))
            using (StreamWriter streamWriterTrue = new StreamWriter(fsTrue, System.Text.Encoding.UTF8))
            {
                if (table.Columns.Count > 0)
                {
                    foreach (DataColumn column in table.Columns)
                    {
                        cols += column.ColumnName.ToString() + ";";
                    }

                    cols = cols.Substring(0, cols.Length - 1);
                    streamWriterTrue.WriteLine(cols);
                }

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

        // Methode zum Schreiben der Tabelle
        public DataTable WriteTable(OdbcCommand command, DataTable table, DataTable columnTable)
        {
            string cmd = command.CommandText;
            DataSet dataSet = new DataSet();
            Type type = typeof(string);
            DataTable cachetable = new DataTable();
            command.CommandTimeout = 30;

            using (OdbcDataReader dataReader = command.ExecuteReader())
            {
                try
                {
                    Stopwatch stopwatch = new Stopwatch();
                    stopwatch.Start();
                    table.Load(dataReader);
                    stopwatch.Stop();
                }
                catch (OdbcException oex)
                {
                }
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

            if (columnTable != null)
            {
                foreach (DataColumn col in clone.Columns)
                {
                    try
                    {
                        col.ColumnName = columnTable.Columns[i].ColumnName.ToString();
                    }
                    catch (Exception e)
                    {
                    }

                    i++;
                }
            }

            return clone;
        }


        // SQL-Abfrage Lieferanten
        protected string LieferantenString(string datum, string standort)
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
                 //"and ans_x_nich_aktiv in ('N','') " +
                 //"and his_kdnlfdnr = kdn_lfdnr " +
                 "order by kdn_kontonr";
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
                    "and ans_typ = 'K' " +
                    "and kdn_info_001 not like ('gelöscht') " +
                    "and kdn_info_001 not like ('gesperrt') ";
            }

            return query;
        }

        // SQL-Abfrage Kunden
        protected string KundenString(string datum, string standort)
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
                          "GROUP BY kdn_kontonr ";
            }

            else if (standort == "2")
            {
                query = "SELECT * from artikel limit 10"; 
                    /*       
                    "kunden.kdn_kontonr," +                         //0
                           "'@' as Profitcenter, " +
                           "anschrift.ansnr, " +
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
                           "'', " +                      //50 
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
                           "'@' AS Expr50, " +
                           "'@' AS Expr51, " +
                           "'@' AS Expr52, " +
                           "'', " +                        //90
                           "'', " +
                           "'0', " +
                           "kdn_x_ust_wann " +
                           "FROM kunden, anschrift " +
                           "WHERE (anschrift.ansnr = kunden.kdn_lfdnr) " +
                           "and (kunden.kdn_sperrkz = 0) " +
                           "and kdn_typ = 'D' " +
                           //"and kdn_aenderung > " + datum + " " +
                           "order BY kdn_kontonr ";
                    */
            }

            return query;
        }
    }
}



