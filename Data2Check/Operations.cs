using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Windows;
using System.Xml;

namespace Data2Check
{
    public class Operations
    {
        DataTable kobensen = new DataTable();
        public Operations()
        {

        }

        //Tabelle schreiben
        public DataTable WriteTable(OdbcCommand command, DataTable table)
        {

            DataTable dataTable = table.Copy();
            string cmd = command.CommandText;
            DataSet dataSet = new DataSet();
            Type type = typeof(string);
            DataTable cachetable = new DataTable();
            DataTable columntable = Tables.s_Kunde.Clone();
            command.CommandText = cmd;
            command.CommandTimeout = 30;

            if (command.Connection.State != ConnectionState.Open)
            {
                command.Connection.Open();
            }

            try
            {
                table.Load(command.ExecuteReader());
            }

            catch (Exception oex)
            {

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
                try
                {
                    clone.ImportRow(row);
                }
                catch (Exception ex)
                {

                }
            }
            if (columntable != null)
            {
                foreach (DataColumn col in clone.Columns)
                {

                    try
                    {
                        col.ColumnName = columntable.Columns[i].ColumnName.ToString();
                    }
                    catch (Exception e)
                    {

                    }

                    i++;
                }
            }

            return clone;
        }

        //Kobensenliste füllen
        public void FillUstidKobensen(DataTable kobensen)
        {
            int count = 0;

            string KobensenFile = @"C:\Users\Admin.TanPat\source\repos\tanYmann\Data2Check\Data2Check\Kobensen_Anhang_B_I.csv";

            FileStream streamIn = new FileStream(KobensenFile, FileMode.Open, FileAccess.Read);

            StreamReader sr = new StreamReader(streamIn);

            string[] field = new string[9];

            while (!sr.EndOfStream)
            {
                string line = sr.ReadLine();

                try
                {
                    field[0] = line.Split(';')[0];
                    field[1] = line.Split(';')[1];
                    field[2] = line.Split(';')[2];
                    field[3] = line.Split(';')[3];
                    field[4] = line.Split(';')[4];
                    field[5] = line.Split(';')[5];
                    field[6] = line.Split(';')[6];
                    field[7] = line.Split(';')[7];
                    field[8] = line.Split(';')[8];
                }
                catch (Exception e)
                {

                }

                if (count == 0)
                {
                    foreach (string entry in field)
                    {
                        kobensen.Columns.Add(entry, typeof(string));
                    }

                    kobensen.PrimaryKey = new DataColumn[] { kobensen.Columns[field[0]] };

                    count++;

                }
                else
                {
                    DataRow row = kobensen.NewRow();

                    try
                    {
                        int countF = 0;

                        foreach (string entry in field)
                        {
                            row.SetField(countF, entry);

                            countF++;
                        }

                        kobensen.Rows.Add(row);
                    }
                    catch (Exception ex)
                    {


                    }
                }

            }
        }

        //Pfad Datei letztes Exportdatum
        public FileInfo FileInfoDate = new FileInfo(Directory.GetCurrentDirectory().ToString() + "\\LastDate.txt");


        public string Street { get; set; }
        public string HNr { get; set; }
        public string Date { get; set; }

        //Zahlen in Liste 0-9
        public static List<string> ListZahlen = new List<string>()
        {
            {"0"},
            {"1"},
            {"2"},
            {"4"},
            {"5"},
            {"6"},
            {"7"},
            {"8"},
            {"9"}
        };

        // Logger zum Aufzeichnen von Ereignissen
        public static void Logger(string error)
        {
            using (FileStream fs = new FileStream(@"C:\tmp\data2checkErrorlog.txt", FileMode.OpenOrCreate, FileAccess.ReadWrite))
            using (StreamReader reader = new StreamReader(fs))
            using (StreamWriter writer = new StreamWriter(fs))
            {
                writer.BaseStream.Position = reader.BaseStream.Length;
                writer.WriteLine("[" + DateTime.Now + "] : ");
                writer.WriteLine(error + "\n");
            }
        }

        // Datumsstring
        public string GetLastDate()
        {
            using (FileStream fstream = new FileStream(Directory.GetCurrentDirectory().ToString() + "\\LastDate.txt", FileMode.Open, FileAccess.Read))
            using (StreamReader sreader = new StreamReader(fstream))
            {
                Date = (long.Parse(sreader.ReadLine()) - 1).ToString();
            }

            return Date;
        }

        // Führende Nullen hinzufügen
        string AddZero(string datepart)
        {
            if (int.Parse(datepart) < 10)
            {
                datepart = "0" + datepart;
            }

            return datepart;
        }

        // Setzen des Datumsstrings
        public void SetLastDate(FileInfo info)
        {
            using (FileStream fstream = info.OpenWrite())
            using (StreamWriter writer = new StreamWriter(fstream))
            {
                writer.BaseStream.Position = 0;
                writer.WriteLine(DateTime.Now.Year.ToString() + AddZero(DateTime.Now.Month.ToString()) + AddZero(DateTime.Now.Day.ToString()));
            }
        }

        // -----------------------------------------------Entfernen sämtlicher Leerzeichen in einem String
        public string RemoveWhiteSpace(string text)
        {
            text = text.Replace(" ", "");

            return text;
        }

        // -----------------------------------------------Stringtrimmer auf max. 35 Zeichen
        public void NameTrimmer35(string one, string two, string three, string four)
        {
            int count = 0;
            string[] strings = new string[4];
            strings[0] = one;
            strings[1] = two;
            strings[2] = three;
            strings[3] = four;

            foreach (string str in strings)
            {
                string nameOut = str;

                while (nameOut.Length > 35)
                {
                    if (nameOut.Contains(' '))
                    {
                        nameOut = nameOut.Substring(0, nameOut.LastIndexOf(' '));
                    }
                    else
                    {
                        nameOut = nameOut.Substring(0, 35);
                    }
                }

                strings[count] = nameOut;

                switch (count)
                {
                    case (0):
                        one = nameOut;
                        break;
                    case (1):
                        two = nameOut;
                        break;
                    case (2):
                        three = nameOut;
                        break;
                    case (3):
                        four = nameOut;
                        break;
                    default:
                        break;
                }

                count++;
            }
        }

        // Methode zum Korrigieren deutscher Straßennamen
        public string CorrectStrasse(string strasse)
        {
            string str = " str.";
            string strP = " Straße";
            string str2 = "Str.";
            string str2P = "Straße";
            string str3 = "-str.";
            string str3P = "-Straße";
            string str4 = "-Str.";
            string str4P = "-Straße";

            if (strasse.Contains(str))
            {
                strasse = strasse.Replace(str, strP);
            }
            else if (strasse.Contains(str2))
            {
                strasse = strasse.Replace(str2, str2P);
            }
            else if (strasse.Contains(str3))
            {
                strasse = strasse.Replace(str3, str3P);
            }
            else if (strasse.Contains(str4))
            {
                strasse = strasse.Replace(str4, str4P);
            }

            return strasse;
        }

        // Trennung von Straße und Hausnummer
        public static (string street, string houseNumber) GetHnr(string address)
        {
            address = Regex.Replace(address, @"\b([0-9]+)\s*([A-Za-z])\b", "$1");
            var regex = new Regex(@"(?<strasse>.*[^\d\s])\s*(?<hausnummer>\d+(?:[-\s]\d+)?[A-Za-z]?)$");
            var match = regex.Match(address);

            if (match.Success)
            {
                var street = match.Groups["strasse"].Value.Trim();
                var houseNumber = match.Groups["hausnummer"].Value.Trim();
                return (street, houseNumber);
            }
            else
            {
                return (null, null);
            }
        }


        // -----------------------------------------------Setzen der Branchennummer mit drei Ziffern (führende 0)
        public static string SetBrancheNr(string branchennr)
        {
            int nummer = 0;

            try
            {
                nummer = int.Parse(branchennr);
            }
            catch (Exception e)
            {

            }

            string zweifach = "00";
            string einfach = "0";

            if (branchennr.Length > 0)
            {
                if (nummer < 10)
                {
                    branchennr = zweifach + branchennr;
                }

                if (nummer > 9 && nummer < 100)
                {
                    branchennr = einfach + branchennr;
                }
            }
            else
            {
                branchennr = "@";
            }

            return branchennr;
        }

        // -----------------------------------------------Lieferbedingungen Einkauf setzen
        public string EinkaufLbdCheck(string lbdnr, string vsanr)
        {
            try
            {
                if (vsanr == "1")
                {
                    if (Int32.Parse(lbdnr) < 1 | Int32.Parse(lbdnr) > 6 && Int32.Parse(lbdnr) != 8)

                    {
                        lbdnr = "1";
                    }

                    if (Int32.Parse(lbdnr) == 8)
                    {
                        lbdnr = "2";
                    }
                }
                else
                {
                    switch (lbdnr)
                    {
                        case "1":
                            lbdnr = "7";
                            break;
                        case "2":
                            lbdnr = "8";
                            break;
                        case "3":
                            lbdnr = "11";
                            break;
                        case "4":
                            lbdnr = "12";
                            break;
                        default:
                            return lbdnr;
                    }
                }
            }
            catch (Exception ex)
            {

            }
            return lbdnr;
        }

        // -----------------------------------------------Setzen der Nummer des Einkaufsverbands
        public string GetNrVerb(string name, string name2, string vbNr)
        {
            string pattern = @"\d*\d";
            Regex regex = new Regex(pattern);

            if (vbNr == "195160")
            {
                name = name.Replace(" ", "");

                if (name.Contains("L345776M"))
                {
                    name = name.Replace("L345776M", "");
                }
                else
                {
                    name = name2;
                }

                name = name.Replace(" ", "");
                name = name.Replace("L345776M", "");
                name = regex.Match(name).Value.ToString();
            }
            else if (vbNr == "381450")
            {
                string nordwest = "Nordwest";

                if (!name.Contains(nordwest))
                {
                    name = name2;
                }
                try
                {
                    name = name.Substring(0, name.LastIndexOf('-'));
                    name = regex.Match(name).Value.ToString();
                }
                catch (Exception e)
                {
                    name = "";
                }
            }
            else if (vbNr == "194330")
            {
                string evb = "##";

                if (!name.Contains(evb))
                {
                    name = name2;
                }

                name = regex.Match(name).Value.ToString();
            }
            else if (vbNr == "196930")
            {
                string evb = "ZR0135VL74171AH";

                if (!name.Contains("0135"))
                {
                    name = name2;
                }

                name = name.Replace(" ", "");
                name = name.Replace(evb, "");
                name = regex.Match(name).Value.ToString();
            }

            return name;
        }

        // Setzen der Region
        public string GetRegion(string land, string plz)
        {
            string region = "@";
            if (plz.Length > 0)
            {
                if (land == "DE")
                {
                    try
                    {
                        region = Dictionaries.regioDE[plz.Substring(0, 2)];
                    }
                    catch
                    {
                        {
                            region = "EDE";
                        }
                    }
                }
                else if (land == "FR")
                {
                    try
                    {
                        region = Dictionaries.regioFR[plz.Substring(0, 2)];
                    }
                    catch
                    {
                        region = "EFR";
                    }
                }

                else if (land == "AT")
                {
                    try
                    {
                        region = Dictionaries.regioAT[plz.Substring(0, 1)];
                    }
                    catch
                    {
                        region = "EAT";
                    }
                }

                else if (land == "NL")
                {
                    try
                    {
                        region = Dictionaries.regioNL[plz.Substring(0, 1)];
                    }
                    catch
                    {
                        region = "ENL";
                    }
                }

                else if (land == "BE")
                {
                    try
                    {
                        region = Dictionaries.regioBE[plz.Substring(0, 1)];
                    }
                    catch
                    {
                        region = "EBE";
                    }
                }

                else if (land == "CH")
                {
                    try
                    {
                        region = Dictionaries.regioCH[plz.Substring(0, 1)];
                    }
                    catch
                    {
                        region = "ECH";
                    }
                }
                else
                {

                }
            }
            return region;
        }

        /// <summary>
        /// Datumskonvertierung. Aus dem Paritydatum wird ein proAlpha kompatibles.
        /// </summary>
        /// <param name="inDate">Datum Parity</param>
        /// <returns>outDate : Datum proAlpha</returns>        
        public string DateConvert(string inDate)
        {
            string outDate = "@";

            if (inDate != "" && inDate != "0")
            {
                string year = inDate.Substring(0, 4);

                string month = inDate.Substring(4, 2);

                string day = inDate.Substring(6, 2);

                outDate = day + "." + month + "." + year;
            }

            return outDate;
        }

        // -----------------------------------------------Setzen der Preisliste
        public string GetPreisliste(string preislistNr)
        {
            string preisliste;

            if (Dictionaries.PrsListDict.ContainsKey(preislistNr))
            {
                preisliste = Dictionaries.PrsListDict[preislistNr];
            }

            else
            {
                preisliste = preislistNr;
            }

            return preisliste;
        }

        // -----------------------------------------------Setzen Kundenrabattgruppe
        public string GetKundenrabattgruppe(string concat)
        {
            string rabattgruppe = "@";

            if (concat.StartsWith("1") || concat.StartsWith("5") || concat.StartsWith("6") || concat.StartsWith("8"))
            {
                rabattgruppe = "010";
            }

            else if ((concat.StartsWith("2_AT") || concat.StartsWith("2_CH") || concat.StartsWith("2_GB") || concat.StartsWith("2_PL")) && concat.EndsWith("_100"))
            {
                rabattgruppe = "030";
            }

            else if (concat.StartsWith("2"))
            {
                rabattgruppe = "020";
            }

            else if (concat.StartsWith("3"))
            {
                rabattgruppe = "040";
            }

            else if (concat.StartsWith("4"))
            {
                rabattgruppe = "050";
            }

            else if (concat.StartsWith("7"))
            {
                rabattgruppe = "060";
            }

            return rabattgruppe;
        }

        // Füllen der Atradiustabelle
        public void FillAtradius(DataTable Atradius)
        {
            int count = 0;
            Atradius = new DataTable();
            string AtradiusFile = @"C:\Users\Admin.TanPat\source\repos\tanYmann\Data2Check\Data2Check\Atradius.csv";
            FileStream streamIn = new FileStream(AtradiusFile, FileMode.Open, FileAccess.Read);
            StreamReader sr = new StreamReader(streamIn);
            string[] field = new string[7];

            while (!sr.EndOfStream)
            {
                string line = sr.ReadLine();
                field[0] = line.Split(';')[0];
                field[1] = line.Split(';')[1];
                field[2] = line.Split(';')[2];
                field[3] = line.Split(';')[3];
                field[4] = line.Split(';')[4];
                field[5] = line.Split(';')[5];

                if (count == 0)
                {
                    foreach (string entry in field)
                    {
                        Atradius.Columns.Add(entry, typeof(string));
                    }
                    count++;

                }

                else
                {
                    DataRow row = Atradius.NewRow();

                    try
                    {
                        int countF = 0;

                        foreach (string entry in field)
                        {
                            row.SetField(countF, entry);
                            countF++;
                        }

                        Atradius.Rows.Add(row);
                    }
                    catch (Exception ex)
                    {
                        MessageBox.Show(ex.Message);
                    }
                }
                Atradius.PrimaryKey = new DataColumn[] { Atradius.Columns[field[0]] };
            }
        }

        public void FillUstidKobensen()
        {
            int count = 0;

            string KobensenFile = @"C:\Users\Admin.TanPat\source\repos\tanYmann\Data2Check\Data2Check\Kobensen_Anhang_B_I.csv";

            FileStream streamIn = new FileStream(KobensenFile, FileMode.Open, FileAccess.Read);

            StreamReader sr = new StreamReader(streamIn);

            string[] field = new string[9];

            while (!sr.EndOfStream)
            {
                string line = sr.ReadLine();

                try
                {
                    field[0] = line.Split(';')[0];
                    field[1] = line.Split(';')[1];
                    field[2] = line.Split(';')[2];
                    field[3] = line.Split(';')[3];
                    field[4] = line.Split(';')[4];
                    field[5] = line.Split(';')[5];
                    field[6] = line.Split(';')[6];
                    field[7] = line.Split(';')[7];
                    field[8] = line.Split(';')[8];
                }
                catch (Exception e)
                {
                    Logger(e.Message + field[0].ToString());

                }

                if (count == 0)
                {
                    foreach (string entry in field)
                    {
                        kobensen.Columns.Add(entry, typeof(string));
                    }

                    kobensen.PrimaryKey = new DataColumn[] { kobensen.Columns[field[0]] };

                    count++;

                }
                else
                {
                    DataRow row = kobensen.NewRow();

                    try
                    {
                        int countF = 0;

                        foreach (string entry in field)
                        {
                            row.SetField(countF, entry);

                            countF++;
                        }

                        kobensen.Rows.Add(row);
                    }
                    catch (Exception ex)
                    {
                        Logger(ex.Message);
                    }
                }

            }
        }
    }
}

